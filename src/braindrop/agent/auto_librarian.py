import json
from typing import Any

from databricks.sdk import WorkspaceClient
from loguru import logger
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig
from braindrop.embedding.vector_search_manager import VectorSearchManager

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are an automated metadata extraction pipeline for a research paper library.\n"
    "\n"
    "You must interact with the system ONLY by executing tool calls. "
    "Do not return conversational text or explanations.\n"
    "\n"
    "Workflow:\n"
    "1. First, call `retrieve_chunks` to read the document context.\n"
    "2. Next, combine the text snippets to extract the metadata.\n"
    "3. Finally, call `write_paper_metadata` to save the results.\n"
    "\n"
    "Fields to extract for `write_paper_metadata`:\n"
    "- title: exact paper title as it appears in the document\n"
    '- authors: list of author names in "Firstname Lastname" format\n'
    '- year: four-digit publication year (e.g., look for "Published:", '
    "copyright notices)\n"
    "- methodology: short phrase for the core technique or field "
    '(e.g., "Retrieval-Augmented Generation")\n'
    "- summary: exactly 3 sentences \u2014 problem, method, key result\n"
    "\n"
    'If a field cannot be determined from the chunks, use "Unknown" for string fields '
    "or an empty array for authors."
)

# ---------------------------------------------------------------------------
# Tool definitions (OpenAI function-calling format)
# ---------------------------------------------------------------------------

TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "retrieve_chunks",
            "description": (
                "Retrieve the first N text chunks for a specific PDF document from the "
                "vector search index. These chunks contain the paper's abstract, title, "
                "and author information. Always call this FIRST before "
                "write_paper_metadata."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "volume_path": {
                        "type": "string",
                        "description": (
                            "The full Databricks Volume path to the PDF file, "
                            "e.g. /Volumes/mlops_dev/galaxyin/paperpile_pdfs/paper.pdf"
                        ),
                    },
                    "num_chunks": {
                        "type": "integer",
                        "description": (
                            "Number of chunks to retrieve. Default 5 is usually "
                            "enough to capture the abstract and front matter."
                        ),
                        "default": 5,
                    },
                },
                "required": ["volume_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_paper_metadata",
            "description": (
                "Write extracted metadata back to the source_pdfs Delta table. "
                "Only call this after retrieve_chunks and after you have extracted "
                "the metadata. Use 'Unknown' for any field you cannot determine "
                "with confidence."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "volume_path": {
                        "type": "string",
                        "description": (
                            "The full volume path, matching the retrieve_chunks call."
                        ),
                    },
                    "title": {
                        "type": "string",
                        "description": "Full paper title as it appears in the document.",
                    },
                    "authors": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "List of author names in 'Firstname Lastname' format."
                        ),
                    },
                    "year": {
                        "type": "integer",
                        "description": "Four-digit publication year, e.g. 2024.",
                    },
                    "methodology": {
                        "type": "string",
                        "description": (
                            "Core methodology or research field, e.g. "
                            "'Retrieval-Augmented Generation', 'Mixture of Experts'."
                        ),
                    },
                    "summary": {
                        "type": "string",
                        "description": (
                            "A 3-sentence summary: (1) what problem is solved, "
                            "(2) what method is used, (3) the key result or contribution."
                        ),
                    },
                },
                "required": [
                    "volume_path",
                    "title",
                    "authors",
                    "year",
                    "methodology",
                    "summary",
                ],
            },
        },
    },
]

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _format_authors_array(authors: list[str]) -> str:
    """Format a Python list of author names as a Spark SQL ARRAY literal."""
    if not authors:
        return "CAST(NULL AS ARRAY<STRING>)"
    escaped = [f"'{a.replace(chr(39), chr(39) + chr(39))}'" for a in authors]
    return f"ARRAY({', '.join(escaped)})"


def _validate_metadata(
    title: str,
    authors: list[str],
    year: int,
    methodology: str,
    summary: str,
) -> None:
    """Raise ValueError if any required field is obviously malformed."""
    if not isinstance(title, str) or not title.strip():
        raise ValueError("title must be a non-empty string")
    if not isinstance(authors, list):
        raise ValueError("authors must be a list")
    if not isinstance(year, int) or not (1900 <= year <= 2030):
        raise ValueError(f"year {year!r} is out of expected range 1900-2030")
    if not isinstance(summary, str) or len(summary.strip()) < 20:
        raise ValueError("summary is too short or empty")
    if not isinstance(methodology, str) or not methodology.strip():
        raise ValueError("methodology must be a non-empty string")


# ---------------------------------------------------------------------------
# AutoLibrarian agent
# ---------------------------------------------------------------------------


class AutoLibrarian:
    """
    An agentic loop that enriches paper metadata in the source_pdfs Delta table.

    The LLM drives the process: it decides when to call retrieve_chunks (to read
    the paper) and when to call write_paper_metadata (to commit the extracted data).
    This demonstrates the tool-calling loop pattern from the LLMOps course.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ProjectConfig,
        llm_endpoint: str = "databricks-llama-4-maverick",
        max_iter: int = 10,
    ) -> None:
        self.spark = spark
        self.config = config
        self.llm_endpoint = llm_endpoint
        self.max_iter = max_iter

        self.vs_manager = VectorSearchManager(spark, config)
        self.w = WorkspaceClient()
        self._openai_client = self.w.serving_endpoints.get_open_ai_client()

        catalog = config.databricks.catalog
        schema = config.databricks.schema_name
        self._source_pdfs_table = f"`{catalog}`.`{schema}`.`source_pdfs`"
        self._chunks_table = f"`{catalog}`.`{schema}`.`chunks_table`"

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def retrieve_chunks(self, volume_path: str, num_chunks: int = 5) -> str:
        """Tool: retrieve document chunks from the vector search index."""
        try:
            results = self.vs_manager.search(
                query="title authors abstract introduction methodology",
                num_results=num_chunks,
                filters={"volume_path": volume_path},
            )
            data_array = results.get("result", {}).get("data_array", [])

            if not data_array:
                # Fallback: direct SQL query on chunks_table
                logger.info(
                    f"Vector search filter returned no results for {volume_path!r}. "
                    "Falling back to direct SQL query on chunks_table."
                )
                return self._retrieve_chunks_via_sql(volume_path, num_chunks)

            chunks = [
                {
                    "chunk_id": row[0],
                    "text": row[1],
                    "authors": row[2],
                    "title": row[3],
                }
                for row in data_array
            ]
            return json.dumps({"volume_path": volume_path, "chunks": chunks})

        except Exception as e:
            logger.warning(
                f"Vector search failed for {volume_path!r}: {e}. "
                "Falling back to direct SQL query."
            )
            return self._retrieve_chunks_via_sql(volume_path, num_chunks)

    def _retrieve_chunks_via_sql(self, volume_path: str, num_chunks: int) -> str:
        """Fallback: retrieve chunks directly from chunks_table via Spark SQL."""
        volume_path_esc = volume_path.replace("'", "''")
        try:
            rows = self.spark.sql(f"""
                SELECT chunk_id, text
                FROM {self._chunks_table}
                WHERE volume_path = '{volume_path_esc}'
                ORDER BY chunk_id
                LIMIT {num_chunks}
            """).collect()

            if not rows:
                return json.dumps(
                    {
                        "error": (
                            f"No chunks found for volume_path={volume_path!r}. "
                            "Verify the path is correct and the pipeline has run."
                        )
                    }
                )

            chunks = [{"chunk_id": row["chunk_id"], "text": row["text"]} for row in rows]
            return json.dumps({"volume_path": volume_path, "chunks": chunks})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def write_paper_metadata(
        self,
        volume_path: str,
        title: str,
        authors: list[str],
        year: int,
        methodology: str,
        summary: str,
    ) -> str:
        """Tool: write extracted metadata to the source_pdfs Delta table."""
        try:
            _validate_metadata(title, authors, year, methodology, summary)

            volume_path_esc = volume_path.replace("'", "''")
            title_esc = title.replace("'", "''")
            summary_esc = summary.replace("'", "''")
            methodology_esc = methodology.replace("'", "''")
            authors_sql = _format_authors_array(authors)

            sql = f"""
                UPDATE {self._source_pdfs_table}
                SET
                    title      = '{title_esc}',
                    authors    = {authors_sql},
                    published  = MAKE_TIMESTAMP({year}, 1, 1, 0, 0, 0),
                    summary    = '{summary_esc}',
                    categories = ARRAY('{methodology_esc}'),
                    updated    = CURRENT_TIMESTAMP()
                WHERE volume_path = '{volume_path_esc}'
                  AND title IS NULL
            """
            result = self.spark.sql(sql)
            num_affected = result.first()["num_affected_rows"] if result else 0

            if num_affected == 0:
                return json.dumps(
                    {
                        "status": "skipped",
                        "reason": "Row not found or already has metadata.",
                        "volume_path": volume_path,
                    }
                )

            logger.info(
                f"Wrote metadata for {volume_path!r} (rows updated: {num_affected})"
            )
            return json.dumps(
                {
                    "status": "success",
                    "volume_path": volume_path,
                    "rows_updated": num_affected,
                }
            )

        except ValueError as e:
            return json.dumps({"status": "error", "reason": str(e)})
        except Exception as e:
            logger.error(f"SQL write failed for {volume_path!r}: {e}")
            return json.dumps({"status": "error", "reason": str(e)})

    # ------------------------------------------------------------------
    # Tool dispatch
    # ------------------------------------------------------------------

    def execute_tool(self, tool_name: str, args: dict) -> str:
        """Dispatch a tool call by name to the appropriate implementation."""
        if tool_name == "retrieve_chunks":
            return self.retrieve_chunks(**args)
        elif tool_name == "write_paper_metadata":
            return self.write_paper_metadata(**args)
        else:
            return json.dumps({"error": f"Unknown tool: {tool_name!r}"})

    # ------------------------------------------------------------------
    # LLM call
    # ------------------------------------------------------------------

    def call_llm(self, messages: list[dict]) -> dict:
        """
        Call the LLM via the OpenAI-compatible Databricks client.

        Returns the assistant message as a plain dict (role, content, tool_calls).
        """
        response = self._openai_client.chat.completions.create(
            model=self.llm_endpoint,
            messages=messages,
            tools=TOOLS,
            tool_choice="required",
        )
        msg = response.choices[0].message

        msg_dict: dict[str, Any] = {
            "role": msg.role,
            "content": msg.content,
        }
        if msg.tool_calls:
            msg_dict["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in msg.tool_calls
            ]
        return msg_dict

    # ------------------------------------------------------------------
    # Agentic loop
    # ------------------------------------------------------------------

    def _run_tool_loop(self, messages: list[dict]) -> bool:
        """
        Core agentic loop: call the LLM, execute any requested tool calls,
        append results to the conversation, and repeat until either:
        - write_paper_metadata completes (success or skipped) — exit immediately
        - the model returns a plain assistant message with no tool calls
        - max_iter is reached

        Returns True if write_paper_metadata was called and confirmed, False otherwise.
        Returning False without raising lets enrich_paper decide whether to retry.
        """
        for iteration in range(self.max_iter):
            logger.info(f"Agent loop iteration {iteration + 1}/{self.max_iter}")
            response_msg = self.call_llm(messages)

            tool_calls = response_msg.get("tool_calls")

            if not tool_calls:
                # LLM returned plain text — write was never called
                logger.warning(
                    f"LLM returned text instead of a tool call "
                    f"(iteration {iteration + 1}). "
                    f"Response: {(response_msg.get('content') or '')[:200]!r}"
                )
                return False

            # Append the assistant message (with tool_calls) to history
            messages.append(response_msg)

            # Execute each tool and feed the result back into the conversation
            for tc in tool_calls:
                tool_name = tc["function"]["name"]
                args = json.loads(tc["function"]["arguments"])
                logger.info(f"Tool call: {tool_name}({args})")
                tool_result = self.execute_tool(tool_name, args)
                logger.info(f"Tool result: {tool_result}")

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": tool_result,
                    }
                )

                # Exit as soon as the write is confirmed — don't ask the LLM again
                if tool_name == "write_paper_metadata":
                    result_data = json.loads(tool_result)
                    if result_data.get("status") in ("success", "skipped"):
                        return True

        logger.warning(
            f"Max iterations ({self.max_iter}) reached without write confirmation."
        )
        return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich_paper(self, volume_path: str, max_retries: int = 3) -> dict:
        """
        Run the Auto-Librarian agent for a single paper, retrying if the model
        fails to call write_paper_metadata (e.g. narrates the call as text).

        Returns a dict with "status" ("success" or "error") and "volume_path".
        """
        logger.info(f"Enriching paper: {volume_path}")
        filename = volume_path.split("/")[-1]

        for attempt in range(1, max_retries + 1):
            if attempt > 1:
                logger.info(f"Retry attempt {attempt}/{max_retries} for {volume_path!r}")

            messages: list[dict] = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Please enrich the metadata for this research paper:\n\n"
                        f"volume_path: {volume_path}\n"
                        f"filename: {filename}\n\n"
                        "Use retrieve_chunks to read the document, extract the title, "
                        "authors, publication year, core methodology, and a 3-sentence "
                        "summary. Then call write_paper_metadata to save the results."
                    ),
                },
            ]

            try:
                write_confirmed = self._run_tool_loop(messages)
            except Exception as e:
                logger.error(
                    f"Attempt {attempt}: unexpected error for {volume_path!r}: {e}"
                )
                if attempt == max_retries:
                    return {
                        "status": "error",
                        "volume_path": volume_path,
                        "error": str(e),
                    }
                continue

            if write_confirmed:
                logger.info(
                    f"Enrichment confirmed for {volume_path!r} (attempt {attempt})"
                )
                return {"status": "success", "volume_path": volume_path}

            logger.warning(
                f"Attempt {attempt}/{max_retries}: write not confirmed "
                f"for {volume_path!r}"
            )

        logger.error(
            f"write_paper_metadata was never called for {volume_path!r} "
            f"after {max_retries} attempts."
        )
        return {
            "status": "error",
            "volume_path": volume_path,
            "error": f"write_paper_metadata not confirmed after {max_retries} attempts",
        }

    def enrich_all_papers(self, limit: int | None = None) -> list[dict]:
        """
        Batch enrich all papers in source_pdfs that have a NULL title or summary.

        Args:
            limit: Optional cap on the number of papers to process in one run.
                   Use this to process large libraries incrementally.

        Returns:
            List of per-paper result dicts from enrich_paper().
        """
        query = f"""
            SELECT volume_path
            FROM {self._source_pdfs_table}
            WHERE title IS NULL OR summary IS NULL
            ORDER BY updated ASC
            {"LIMIT " + str(limit) if limit is not None else ""}
        """
        rows = self.spark.sql(query).collect()
        volume_paths = [row["volume_path"] for row in rows]
        logger.info(f"Found {len(volume_paths)} papers to enrich.")

        results = []
        for vp in volume_paths:
            result = self.enrich_paper(vp)
            results.append(result)

        successes = sum(1 for r in results if r["status"] == "success")
        logger.info(
            f"Batch complete: {successes}/{len(results)} papers enriched successfully."
        )
        return results
