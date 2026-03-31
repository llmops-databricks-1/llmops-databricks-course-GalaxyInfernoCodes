"""
Helper script for debugging which cleans up
the tables used for data ingestion to have a clean slate.
"""

import argparse
import os
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig


def get_spark() -> SparkSession:
    """Get or create a Spark session."""
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def main() -> None:
    # 0. Parse CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_config_path",
        type=str,
        default="project_config.yaml",
        help="Path to project_config.yaml",
    )
    args, unknown = parser.parse_known_args()

    # Resolve and load config
    config_path = Path(args.project_config_path)
    config = ProjectConfig.load(config_path)

    catalog = config.databricks.catalog
    schema = config.databricks.schema_name
    volume = config.databricks.volume

    source_table = f"`{catalog}`.`{schema}`.`source_pdfs`"
    parsed_table = f"`{catalog}`.`{schema}`.`ai_parsed_docs_table`"
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

    spark = get_spark()

    # 1. Truncate Tables
    logger.info(f"Emptying table: {source_table}")
    try:
        spark.sql(f"TRUNCATE TABLE {source_table}")
    except Exception as e:
        logger.warning(f"Could not truncate {source_table} (it might not exist): {e}")

    logger.info(f"Emptying table: {parsed_table}")
    try:
        spark.sql(f"TRUNCATE TABLE {parsed_table}")
    except Exception as e:
        logger.warning(f"Could not truncate {parsed_table} (it might not exist): {e}")

    # 2. Clear Volume
    logger.info(f"Deleting files from volume: {volume_path}")

    # Try using dbutils if available (most reliable on Databricks)
    try:
        from databricks.sdk.runtime import dbutils

        files = dbutils.fs.ls(volume_path)
        for f in files:
            logger.info(f"Deleting {f.path}")
            dbutils.fs.rm(f.path)
    except ImportError:
        # Fallback to standard os for local or FUSE-mounted environments
        if os.path.exists(volume_path):
            for filename in os.listdir(volume_path):
                file_path = os.path.join(volume_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        logger.info(f"Deleted {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}. Reason: {e}")
        else:
            logger.warning(f"Volume path {volume_path} does not exist locally.")

    logger.success("Cleanup complete. You can now run the import scripts again.")


if __name__ == "__main__":
    main()
