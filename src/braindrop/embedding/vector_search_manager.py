from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from databricks.vector_search.index import VectorSearchIndex
from loguru import logger
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig


class VectorSearchManager:
    """Manages vector search endpoints and indexes for document chunks."""

    def __init__(self, spark: SparkSession, config: ProjectConfig) -> None:
        """Initialize VectorSearchManager.

        Args:
            spark: SparkSession instance
            config: ProjectConfig object
        """
        self.spark = spark
        self.config = config
        self.catalog = config.databricks.catalog
        self.schema = config.databricks.schema_name
        self.endpoint_name = config.databricks.vector_search_endpoint
        self.embedding_model = config.databricks.embedding_endpoint

        # Get credentials from WorkspaceClient for authentication
        w = WorkspaceClient()
        self.vs_client = VectorSearchClient(
            workspace_url=w.config.host,
            personal_access_token=w.tokens.create(lifetime_seconds=1200).token_value,
        )
        self.index_name = f"{self.catalog}.{self.schema}.braindrop_index"

    def create_endpoint_if_not_exists(self) -> None:
        """Create vector search endpoint if it doesn't exist."""
        endpoints_response = self.vs_client.list_endpoints()
        endpoints = (
            endpoints_response.get("endpoints", [])
            if isinstance(endpoints_response, dict)
            else []
        )
        endpoint_exists = any(
            (ep.get("name") if isinstance(ep, dict) else getattr(ep, "name", None))
            == self.endpoint_name
            for ep in endpoints
        )

        if not endpoint_exists:
            logger.info(f"Creating vector search endpoint: {self.endpoint_name}")
            self.vs_client.create_endpoint_and_wait(
                name=self.endpoint_name,
                endpoint_type="STANDARD",
            )
            logger.info(f"✓ Vector search endpoint created: {self.endpoint_name}")
        else:
            logger.info(f"✓ Vector search endpoint exists: {self.endpoint_name}")

    def create_or_get_index(self) -> VectorSearchIndex:
        """Create or get vector search index.

        Returns:
            Vector search index object
        """
        self.create_endpoint_if_not_exists()
        source_table = f"{self.catalog}.{self.schema}.chunks_table"

        # Try to get existing index
        try:
            index = self.vs_client.get_index(
                endpoint_name=self.endpoint_name, index_name=self.index_name
            )
            logger.info(f"✓ Vector search index exists: {self.index_name}")
            return index
        except Exception:
            logger.info(f"Index {self.index_name} not found, will create it")

        # Try to create the index
        try:
            index = self.vs_client.create_delta_sync_index(
                endpoint_name=self.endpoint_name,
                source_table_name=source_table,
                index_name=self.index_name,
                pipeline_type="TRIGGERED",
                primary_key="id",
                embedding_source_column="text",
                embedding_model_endpoint_name=self.embedding_model,
            )
            logger.info(f"✓ Vector search index created: {self.index_name}")
            return index
        except Exception as e:
            if "RESOURCE_ALREADY_EXISTS" not in str(e):
                raise
            # Index exists but get_index failed earlier (transient) — retry
            logger.info(f"✓ Vector search index exists: {self.index_name}")
            return self.vs_client.get_index(
                endpoint_name=self.endpoint_name, index_name=self.index_name
            )

    def sync_index(self) -> None:
        """Sync the vector search index with the source table."""
        index = self.create_or_get_index()
        logger.info(f"Syncing vector search index: {self.index_name}")
        index.sync()
        logger.info("✓ Index sync triggered")

    def search(
        self, query: str, num_results: int = 5, filters: dict | None = None
    ) -> dict:
        """Search the vector index.

        Args:
            query: Search query text
            num_results: Number of results to return
            filters: Optional filters to apply

        Returns:
            Search results dictionary
        """
        index = self.vs_client.get_index(
            endpoint_name=self.endpoint_name, index_name=self.index_name
        )
        results = index.similarity_search(
            query_text=query,
            columns=["id", "text", "authors", "title"],
            num_results=num_results,
            filters=filters,
        )
        return results
