import json
import re

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    explode,
    regexp_replace,
    udf,
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from braindrop.config import ProjectConfig


class ChunksManager:
    """
    ChunksManager handles extracting text elements from JSON-parsed documents,
    cleaning the text, and storing it in a structured Delta table for vector search.
    """

    def __init__(self, spark: SparkSession, config: ProjectConfig) -> None:
        """
        Initialize ChunksManager.

        Args:
            spark: SparkSession instance
            config: ProjectConfig object
        """
        self.spark = spark
        self.config = config
        self.catalog = config.databricks.catalog
        self.schema = config.databricks.schema_name
        self.parsed_table = f"`{self.catalog}`.`{self.schema}`.`ai_parsed_docs_table`"
        self.chunks_table = f"`{self.catalog}`.`{self.schema}`.`chunks_table`"
        self.source_table = f"`{self.catalog}`.`{self.schema}`.`source_pdfs`"

    @staticmethod
    def _extract_chunks(parsed_content_json: str) -> list[tuple[str, str]]:
        """
        Extract chunks from parsed_content JSON string returned by ai_parse_document.
        """
        if not parsed_content_json:
            return []

        try:
            parsed_dict = json.loads(parsed_content_json)
            chunks = []

            # Extract only text elements
            for element in parsed_dict.get("document", {}).get("elements", []):
                if element.get("type") == "text":
                    chunk_id = element.get("id", "")
                    content = element.get("content", "")
                    if content.strip():
                        chunks.append((chunk_id, content))

            return chunks
        except Exception as e:
            logger.error(f"Error parsing JSON content: {e}")
            return []

    @staticmethod
    def _clean_chunk(text: str) -> str:
        """
        Clean and normalize chunk text:
        - Join hyphenated words across line breaks
        - Collapse newlines and extra spaces
        """
        if not text:
            return ""

        # Fix hyphenation: "docu-\nments" => "documents"
        t = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)
        # Collapse internal newlines and repeated whitespace into single spaces
        t = re.sub(r"\s*\n\s*", " ", t)
        t = re.sub(r"\s+", " ", t)

        return t.strip()

    def process_chunks(self) -> None:
        """
        Process parsed documents to extract and clean chunks.
        Joins with source metadata and saves to chunks_table.
        """
        logger.info(f"Processing parsed documents from {self.parsed_table}")

        # Ensure the parsed table exists
        if not self.spark.catalog.tableExists(self.parsed_table):
            logger.error(f"Table {self.parsed_table} does not exist. Run parsing first.")
            return

        # 1. Define UDFs
        chunk_schema = ArrayType(
            StructType(
                [
                    StructField("chunk_id", StringType(), True),
                    StructField("content", StringType(), True),
                ]
            )
        )
        extract_chunks_udf = udf(self._extract_chunks, chunk_schema)
        clean_chunk_udf = udf(self._clean_chunk, StringType())

        # 2. Transform: Parse JSON -> Explode -> Clean -> Join
        parsed_df = self.spark.table(self.parsed_table)

        chunks_df = (
            parsed_df.withColumn("volume_path", regexp_replace(col("path"), "^dbfs:", ""))
            .withColumn("extracted", extract_chunks_udf(col("parsed_content")))
            .withColumn("chunk", explode(col("extracted")))
            .select(
                col("volume_path"),
                col("chunk.chunk_id").alias("chunk_id"),
                clean_chunk_udf(col("chunk.content")).alias("text"),
                # Create a unique ID for vector search indexing
                concat_ws("_", col("volume_path"), col("chunk.chunk_id")).alias("id"),
            )
        )

        # 3. Save to Delta table
        logger.info(f"Saving chunks to {self.chunks_table}")
        chunks_df.write.mode("overwrite").saveAsTable(self.chunks_table)

        # 4. Enable Change Data Feed for the Vector Search index
        self.spark.sql(
            f"ALTER TABLE {self.chunks_table} "
            "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
        logger.success(
            f"Successfully processed chunks and enabled CDF on {self.chunks_table}"
        )
