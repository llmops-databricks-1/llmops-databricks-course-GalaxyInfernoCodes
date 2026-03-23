import argparse
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig
from braindrop.ingestion.volume_storage import (
    ensure_volume_exists,
    upload_local_file_to_volume,
)


def get_spark() -> SparkSession:
    """Get or create a Spark session via Databricks Connect."""
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        print("Databricks Connect not found, falling back to local SparkSession.")
        return SparkSession.builder.getOrCreate()


def main() -> None:
    load_dotenv()

    # 0. Parse CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_config_path",
        type=str,
        default="project_config.yaml",
        help="Path to project_config.yaml",
    )
    parser.add_argument(
        "--root_path", type=str, help="Root path of the project in Databricks Workspace"
    )

    # Use parse_known_args to ignore extra parameters passed by Databricks jobs
    args, unknown = parser.parse_known_args()

    if unknown:
        print(f"Ignored unknown arguments: {unknown}")

    # Resolve the config path
    config_path = Path(args.project_config_path)
    if args.root_path:
        config_path = Path(args.root_path) / args.project_config_path

    # Reload config if path provided
    config = ProjectConfig.load(config_path)

    # Configuration
    catalog = config.databricks.catalog
    schema = config.databricks.schema_name
    volume = config.databricks.volume

    # 1. Setup Spark
    spark = get_spark()

    # 2. Ensure Volume Exists
    ensure_volume_exists(spark, catalog, schema, volume)

    # 3. Find a PDF in artifacts/
    # If running locally, project_root is parent of scripts/
    # If running on Databricks, artifacts_dir relative to where files are downloaded
    project_root = (
        Path(args.root_path) if args.root_path else Path(__file__).parent.parent
    )
    artifacts_dir = project_root / config.local.artifacts_dir

    pdf_files = list(artifacts_dir.glob("*.pdf"))
    if not pdf_files:
        print(
            f"No PDF files found in {artifacts_dir}. "
            "Please run download_pdfs_from_gdrive.py first."
        )
        return

    # 4. Upload the first PDF found
    target_pdf = pdf_files[0]
    upload_local_file_to_volume(spark, target_pdf, catalog, schema, volume)


if __name__ == "__main__":
    main()
