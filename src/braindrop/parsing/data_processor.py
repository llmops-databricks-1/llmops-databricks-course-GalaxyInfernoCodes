"""
PDFs in Volume + arxiv_papers table
   ↓ (parse_pdfs_with_ai)
ai_parsed_docs_table (JSON)
   ↓ (process_chunks)
arxiv_chunks_table (clean text + metadata)
   ↓ (VectorSearchManager - separate class) (2.4 notebook)
Vector Search Index (embeddings)
"""

from loguru import logger
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig


class DataProcessor:
    """
    DataProcessor handles the complete workflow of:
    - Storing paper metadata
    - Parsing PDFs with ai_parse_document
    - Extracting and cleaning text chunks
    - Saving chunks to Delta tables
    """

    def __init__(self, spark: SparkSession, config: ProjectConfig) -> None:
        """
        Initialize DataProcessor with Spark session and configuration.

        Args:
            spark: SparkSession instance
            config: ProjectConfig object with table configurations
        """
        self.spark = spark
        self.cfg = config
        self.catalog = config.databricks.catalog
        self.schema = config.databricks.schema_name
        self.volume = config.databricks.volume

        self.pdf_dir = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
        # os.makedirs(self.pdf_dir, exist_ok=True)
        self.papers_table = f"{self.catalog}.{self.schema}.arxiv_papers"
        self.parsed_table = f"{self.catalog}.{self.schema}.ai_parsed_docs_table"

    def parse_pdfs_with_ai(self, path: str | None = None) -> None:
        """
        Parse PDFs using ai_parse_document and store in ai_parsed_docs table.
        """
        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema}`")

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.parsed_table} (
                path STRING,
                parsed_content STRING,
                processed LONG
            )
        """)

        pdf_path = path or self.pdf_dir
        self.spark.sql(f"""
            INSERT INTO {self.parsed_table}
            SELECT
                path,
                ai_parse_document(content) AS parsed_content,
                current_timestamp() AS processed
            FROM READ_FILES(
                "{pdf_path}",
                format => 'binaryFile'
            )
        """)

        logger.info(f"Parsed PDFs from {self.pdf_dir} and saved to {self.parsed_table}")

    def get_unparsed_pdfs(self) -> list[str]:
        """Get list of unparsed PDFs from the volume."""

        source_table = f"`{self.catalog}`.`{self.schema}`.`source_pdfs`"
        parsed_table = f"`{self.catalog}`.`{self.schema}`.`ai_parsed_docs_table`"

        # Ensure schema exists before trying to create a table in it
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema}`")

        # Ensure the parsed table exists so the JOIN doesn't fail
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {parsed_table} (
                path STRING,
                parsed_content STRING,
                processed LONG
            )
        """)

        query = f"""
            SELECT s.volume_path
            FROM {source_table} s
            LEFT JOIN {parsed_table} p
            ON s.volume_path = regexp_replace(p.path, '^dbfs:', '')
            WHERE p.path IS NULL
        """

        results = self.spark.sql(query).collect()
        return [row["volume_path"] for row in results]
