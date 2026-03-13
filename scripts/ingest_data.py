import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from braindrop.ingestion.delta_storage import create_arxiv_metadata_table
from braindrop.ingestion.gdrive_retrieval import GDriveClient


def main() -> None:
    load_dotenv()

    # 1. Spark Session setup
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        spark = SparkSession.builder.getOrCreate()

    # 2. Setup Delta Storage
    catalog_name = os.getenv("DATABRICKS_CATALOG", "dev")
    schema_name = os.getenv("DATABRICKS_SCHEMA", "braindrop")
    create_arxiv_metadata_table(spark, catalog_name, schema_name)

    # 3. Setup GDrive Client
    gdrive_client = GDriveClient()

    # 4. List and Process PDF Files
    folder_id = os.getenv("GDRIVE_FOLDER_ID")
    if not folder_id:
        print("GDRIVE_FOLDER_ID not found. Skipping GDrive retrieval.")
    else:
        files = gdrive_client.list_pdf_files(folder_id)
        print(f"Found {len(files)} PDF files in Google Drive folder.")

    spark.stop()


if __name__ == "__main__":
    main()
