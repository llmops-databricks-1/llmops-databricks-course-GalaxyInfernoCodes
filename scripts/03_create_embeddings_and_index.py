"""
Create embeddings and build the Databricks Vector Search index.
"""

import argparse
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig
from braindrop.embedding.vector_search_manager import VectorSearchManager
from braindrop.parsing.chunks_manager import ChunksManager


def get_spark() -> SparkSession:
    """Get or create a Spark session."""
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def main() -> None:
    # Parse CLI Arguments
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
    logger.info(f"Loading config from: {config_path}")
    config = ProjectConfig.load(config_path)

    # Initialize Spark
    spark = get_spark()

    # 1. Process Chunks
    logger.info("Initializing ChunksManager and processing documents...")
    chunks_manager = ChunksManager(spark, config)
    chunks_manager.process_chunks()

    # 2. Initialize VectorSearchManager
    logger.info("Initializing VectorSearchManager...")
    vs_manager = VectorSearchManager(spark, config)

    # 3. Create vector search endpoint if it doesn't exist
    logger.info("Creating vector search endpoint if it doesn't exist...")
    vs_manager.create_endpoint_if_not_exists()

    # 4. Create and Sync Vector Search Index
    logger.info("Creating or syncing vector search index...")
    vs_manager.sync_index()

    logger.info("Vector search index sync triggered successfully.")


if __name__ == "__main__":
    main()
