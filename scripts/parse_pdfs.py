"""
Apply ai_parse_document so specific PDFs to see results and check for Arxiv ID
"""

import argparse
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig
from braindrop.parsing.data_processor import DataProcessor

# Parse CLI Arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "--project_config_path",
    type=str,
    default="project_config.yaml",
    help="Path to project_config.yaml",
)
args, unknown = parser.parse_known_args()

# Resolve the config path
config_path = Path(args.project_config_path)
logger.info(f"Loading config from: {config_path}")

# Reload config from the resolved path
config = ProjectConfig.load(config_path)

try:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()
except ImportError:
    spark = SparkSession.builder.getOrCreate()


# processor = DataProcessor(spark, config)

# Configuration from config
catalog = config.databricks.catalog
schema = config.databricks.schema_name
volume = config.databricks.volume
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
logger.info(f"Searching for PDFs in volume: {volume_path}")

try:
    # Try to list files in the volume to find a PDF
    files = spark.sql(f"LIST '{volume_path}'").collect()
    pdf_file = next((f.path for f in files if f.path.lower().endswith(".pdf")), None)
except Exception as e:
    logger.error(f"Error listing volume: {e}")
    pdf_file = None

processor = DataProcessor(spark, config)

if pdf_file:
    logger.info(f"Found PDF in volume: {pdf_file}")

    processor.parse_pdfs_with_ai(pdf_file)

else:
    logger.error(f"No PDFs found in the volume: {volume_path}")
