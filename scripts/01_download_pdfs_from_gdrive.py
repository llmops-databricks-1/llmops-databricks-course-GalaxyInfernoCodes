"""Download PDF files from Google Drive using Databricks Secrets."""

import argparse
import os
import sys
from pathlib import Path

from loguru import logger

from braindrop.config import ProjectConfig
from braindrop.ingestion.gdrive_retrieval import (
    GDriveClient,
    download_pdfs_to_local,
    download_pdfs_to_volume,
)


def main() -> None:
    """Run the download process."""
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
    parser.add_argument(
        "--max_files",
        type=int,
        default=10,
        help="Maximum number of files to download",
    )

    # Use parse_known_args to ignore extra parameters passed by Databricks jobs
    args, unknown = parser.parse_known_args()

    if unknown:
        logger.info(f"Ignored unknown arguments: {unknown}")

    # Resolve the config path
    config_path = Path(args.project_config_path)
    if args.root_path:
        config_path = Path(args.root_path) / args.project_config_path

    # Reload config from the resolved path
    config = ProjectConfig.load(config_path)

    # 1. Setup Configuration
    folder_id = config.google_drive.folder_id or os.getenv("GDRIVE_FOLDER_ID")
    if not folder_id or folder_id == "replace_with_your_folder_id":
        logger.error("Error: GDRIVE_FOLDER_ID not set correctly.")
        sys.exit(1)

    # 2. Initialize GDrive Client
    gdrive_client = GDriveClient(config=config)

    # 3. Environment Check and Execution
    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if is_databricks:
        logger.info("Running on Databricks. Downloading directly to Volume...")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        from braindrop.ingestion.volume_storage import ensure_volume_exists

        ensure_volume_exists(
            spark,
            config.databricks.catalog,
            config.databricks.schema_name,
            config.databricks.volume,
        )

        download_pdfs_to_volume(
            folder_id,
            config.databricks.catalog,
            config.databricks.schema_name,
            config.databricks.volume,
            config=config,
            spark=spark,
            client=gdrive_client,
            max_files=args.max_files,
        )
    else:
        logger.info("Running locally. Downloading to local artifacts directory...")
        # Use root_path if provided, otherwise project root relative to script
        project_root = (
            Path(args.root_path) if args.root_path else Path(__file__).parent.parent
        )
        artifacts_dir = project_root / config.local.artifacts_dir

        download_pdfs_to_local(
            folder_id,
            artifacts_dir,
            config=config,
            client=gdrive_client,
            max_files=args.max_files,
        )


if __name__ == "__main__":
    main()
