import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from braindrop.config import ProjectConfig
from braindrop.ingestion.gdrive_retrieval import (
    GDriveClient,
    download_pdfs_to_local,
    download_pdfs_to_volume,
)


def main() -> None:
    """Download PDF files from Google Drive using Databricks Secrets."""
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

    # Reload config from the resolved path
    config = ProjectConfig.load(config_path)

    # 1. Setup Configuration
    folder_id = config.google_drive.folder_id or os.getenv("GDRIVE_FOLDER_ID")
    if not folder_id or folder_id == "replace_with_your_folder_id":
        print("Error: GDRIVE_FOLDER_ID not set correctly.")
        return

    # 2. Fetch Google Drive credentials from Databricks Secrets
    try:
        from databricks.sdk.runtime import dbutils

        print(
            f"Fetching credentials from Databricks Secrets "
            f"(scope: {config.databricks.secrets_scope}, "
            f"key: {config.databricks.gdrive_secret_key})..."
        )

        raw_json_str = dbutils.secrets.get(
            scope=config.databricks.secrets_scope, key=config.databricks.gdrive_secret_key
        )
        credentials_info = json.loads(raw_json_str)
        gdrive_client = GDriveClient(service_account_info=credentials_info)
    except (ImportError, AttributeError, Exception) as e:
        print(f"Could not fetch secrets from Databricks: {e}")
        print("Falling back to environment-based credentials.")
        gdrive_client = GDriveClient()

    # 3. Environment Check and Execution
    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if is_databricks:
        print("Running on Databricks. Downloading directly to Volume...")
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
            spark=spark,
            client=gdrive_client,
        )
    else:
        print("Running locally. Downloading to local artifacts directory...")
        # Use root_path if provided, otherwise project root relative to script
        project_root = (
            Path(args.root_path) if args.root_path else Path(__file__).parent.parent
        )
        artifacts_dir = project_root / config.local.artifacts_dir

        download_pdfs_to_local(folder_id, artifacts_dir, client=gdrive_client)


if __name__ == "__main__":
    main()
