from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig


class MetadataManager:
    def __init__(self, spark: SparkSession, config: ProjectConfig) -> None:
        self.spark = spark
        self.config = config
        self.table_path = (
            f"`{self.config.databricks.catalog}`."
            f"`{self.config.databricks.schema_name}`.`source_pdfs`"
        )

    def create_table(self) -> None:
        """Create the Metadata table in Unity Catalog."""
        sql_create = f"""
        CREATE TABLE IF NOT EXISTS {self.table_path} (
            volume_path STRING COMMENT 'Path to the PDF file in the volume',
            gdrive_file_id STRING COMMENT 'ID of the PDF file in Google Drive',
            title STRING COMMENT 'Paper title',
            authors ARRAY<STRING> COMMENT 'List of authors',
            summary STRING COMMENT 'Abstract/Summary',
            published TIMESTAMP COMMENT 'Publication date',
            updated TIMESTAMP COMMENT 'Last update date',
            links ARRAY<STRING> COMMENT 'Links to the paper',
            categories ARRAY<STRING> COMMENT 'Arxiv categories'
        ) USING DELTA
        """
        self.spark.sql(sql_create)

    def insert_pdf(self, pdf_path: str, gdrive_file_id: str) -> None:
        """Insert a PDF into the Metadata table."""
        # Escape single quotes for SQL string literal
        pdf_path_sql = pdf_path.replace("'", "''")
        gdrive_file_id_sql = gdrive_file_id.replace("'", "''")

        sql_insert = f"""
        INSERT INTO {self.table_path} (
            volume_path,
            gdrive_file_id,
            title,
            authors,
            summary,
            published,
            updated,
            links,
            categories
        ) VALUES (
            '{pdf_path_sql}',
            '{gdrive_file_id_sql}',
            NULL,
            NULL,
            NULL,
            NULL,
            CURRENT_TIMESTAMP(),
            NULL,
            NULL
        )
        """
        self.spark.sql(sql_insert)

    def does_pdf_exist(self, gdrive_file_id: str) -> bool:
        """Check if a PDF already exists in the Metadata table."""
        # Escape single quotes for SQL string literal
        gdrive_file_id_sql = gdrive_file_id.replace("'", "''")

        sql_check = f"""
            SELECT COUNT(*) > 0
            FROM {self.table_path}
            WHERE gdrive_file_id = '{gdrive_file_id_sql}'
        """
        result = self.spark.sql(sql_check).collect()
        return result[0][0] if result else False
