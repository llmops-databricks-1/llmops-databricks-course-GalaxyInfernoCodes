"""Delta storage module for Arxiv metadata."""

from pyspark.sql import SparkSession


def create_arxiv_metadata_table(
    spark: SparkSession, catalog_name: str, schema_name: str
) -> None:
    """Create the Arxiv metadata table in Unity Catalog."""
    table_path = f"`{catalog_name}`.`{schema_name}`.`arxiv_metadata`"

    # Define SQL for creating the table
    sql_create = f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
        id STRING COMMENT 'Arxiv ID',
        title STRING COMMENT 'Paper title',
        authors ARRAY<STRING> COMMENT 'List of authors',
        summary STRING COMMENT 'Abstract/Summary',
        published TIMESTAMP COMMENT 'Publication date',
        updated TIMESTAMP COMMENT 'Last update date',
        links ARRAY<STRING> COMMENT 'Links to the paper',
        categories ARRAY<STRING> COMMENT 'Arxiv categories',
        gdrive_file_id STRING COMMENT 'ID of the PDF file in Google Drive'
    ) USING DELTA
    """
    spark.sql(sql_create)
