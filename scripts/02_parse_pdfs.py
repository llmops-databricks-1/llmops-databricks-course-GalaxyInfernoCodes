"""
Apply ai_parse_document so specific PDFs to see results and check for Arxiv ID
"""

import argparse
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession
from tqdm import tqdm

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

processor = DataProcessor(spark, config)

try:
    unparsed_pdf_paths = processor.get_unparsed_pdfs()
except Exception as e:
    logger.error(f"Error listing volume: {e}")
    unparsed_pdf_paths = []

logger.info(f"Found {len(unparsed_pdf_paths)} unparsed PDFs")

for pdf_file in tqdm(unparsed_pdf_paths, desc="Parsing PDFs"):
    logger.info(f"Parsing PDF: {pdf_file}")

    processor.parse_pdfs_with_ai(pdf_file)
