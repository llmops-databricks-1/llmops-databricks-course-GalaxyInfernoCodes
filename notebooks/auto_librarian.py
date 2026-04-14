# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-Librarian Agent
# MAGIC
# MAGIC This notebook demonstrates the **tool-calling loop** pattern: a language model
# MAGIC decides which tools to invoke and in what order, rather than following a fixed
# MAGIC sequence of code steps.
# MAGIC
# MAGIC ## What the agent does
# MAGIC
# MAGIC The `source_pdfs` Delta table is populated by the ingestion pipeline with only
# MAGIC `volume_path` and `gdrive_file_id`. All metadata fields (`title`, `authors`,
# MAGIC `summary`, `published`, `categories`) start as `NULL`.
# MAGIC
# MAGIC The Auto-Librarian fills this gap by running an agentic loop for each paper:
# MAGIC
# MAGIC 1. **Tool: `retrieve_chunks`** — queries the Vector Search index for the
# MAGIC    paper's front-matter chunks (abstract, title page, introduction)
# MAGIC 2. **Tool: `write_paper_metadata`** — writes the extracted metadata back to
# MAGIC    the `source_pdfs` Delta table via a SQL `UPDATE`
# MAGIC
# MAGIC The LLM decides when to call each tool. Once the write is confirmed, it stops.
# MAGIC
# MAGIC > **Prerequisite:** The vector search index must be synced before running this
# MAGIC > notebook. If you just ingested new papers, run script
# MAGIC > `03_create_embeddings_and_index.py` (or `task create-embeddings`) first.

# COMMAND ----------

from pyspark.sql import SparkSession

from braindrop.agent.auto_librarian import AutoLibrarian
from braindrop.config import ProjectConfig


def get_spark() -> SparkSession:
    """Get or create a Spark session."""
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


spark = get_spark()
config = ProjectConfig.load()
agent = AutoLibrarian(spark=spark, config=config)

print(
    f"Auto-Librarian ready.\n"
    f"  Catalog : {config.databricks.catalog}\n"
    f"  Schema  : {config.databricks.schema_name}\n"
    f"  LLM     : databricks-llama-4-maverick"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect unenriched papers
# MAGIC
# MAGIC These rows have been registered by the ingestion pipeline but have not yet
# MAGIC had their metadata populated.

# COMMAND ----------

unenriched_df = spark.sql(f"""
    SELECT volume_path, gdrive_file_id, title, summary
    FROM `{config.databricks.catalog}`.`{config.databricks.schema_name}`.`source_pdfs`
    WHERE title IS NULL OR summary IS NULL
    ORDER BY updated ASC
""")

print(f"Papers awaiting enrichment: {unenriched_df.count()}")
display(unenriched_df)  # type: ignore[name-defined] # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single-paper demo
# MAGIC
# MAGIC Enrich one paper to see the full agentic loop in action.
# MAGIC Watch the `loguru` output to see each tool call the LLM decides to make.

# COMMAND ----------

first_row = unenriched_df.first()

if first_row is None:
    print("No unenriched papers found — all metadata is already populated.")
else:
    sample_path = first_row["volume_path"]
    print(f"Enriching: {sample_path}\n")

    result = agent.enrich_paper(sample_path)
    print(f"\nResult: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the write

# COMMAND ----------

if first_row is not None:
    first_row_df = spark.sql(f"""
        SELECT volume_path, title, authors, summary, categories, published
        FROM `{config.databricks.catalog}`.`{config.databricks.schema_name}`.`source_pdfs`
        WHERE volume_path = '{sample_path.replace("'", "''")}'
    """)
    display(first_row_df)  # type: ignore[name-defined] # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch enrichment
# MAGIC
# MAGIC Process all unenriched papers. Use `limit` to work through large libraries
# MAGIC incrementally — the `WHERE title IS NULL` guard means re-running is safe.

# COMMAND ----------

results = agent.enrich_all_papers(limit=10)

successes = [r for r in results if r["status"] == "success"]
failures = [r for r in results if r["status"] != "success"]

print(f"Batch complete: {len(successes)} succeeded, {len(failures)} failed.")
if failures:
    for f in failures:
        print(f"  FAILED: {f['volume_path']} — {f.get('error', 'unknown error')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final state of the library

# COMMAND ----------

display(  # type: ignore[name-defined] # noqa: F821
    spark.sql(f"""
    SELECT
        volume_path,
        title,
        authors,
        summary,
        categories,
        published
    FROM `{config.databricks.catalog}`.`{config.databricks.schema_name}`.`source_pdfs`
    ORDER BY updated DESC
""")
)

# COMMAND ----------
