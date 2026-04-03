from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from loguru import logger
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabricksConfig(BaseModel):
    catalog: str = "dev"
    schema_name: str = Field(default="braindrop", alias="schema")
    volume: str = "arxiv_pdfs"
    secrets_scope: str = "google-auth"
    gdrive_secret_key: str = "drive-api-key"
    vector_search_endpoint: str = "llmops_course_vs_endpoint"
    embedding_endpoint: str = "databricks-bge-endpoint-v2"


class GoogleDriveConfig(BaseModel):
    folder_id: str = "replace_with_your_folder_id"


class LocalConfig(BaseModel):
    artifacts_dir: str = "artifacts"


class ProjectConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="BRAINDROP_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    databricks: DatabricksConfig = DatabricksConfig()
    google_drive: GoogleDriveConfig = GoogleDriveConfig()
    local: LocalConfig = LocalConfig()

    @classmethod
    def load(cls, config_path: str | Path | None = None) -> "ProjectConfig":
        """Load configuration from YAML file and environment variables."""
        found_path = None

        if config_path is not None:
            p = Path(config_path)
            if p.exists():
                found_path = p
            else:
                # If not found at provided path, don't crash, just warn and try defaults
                logger.warning(f"Warning: Config file not found at provided path: {p}")

        if found_path is None:
            # Default lookup logic
            potential_paths = [
                Path(__file__).parent.parent.parent / "project_config.yaml",
                Path.cwd() / "project_config.yaml",
            ]
            for p in potential_paths:
                if p.exists():
                    found_path = p
                    break

        yaml_data: dict[str, Any] = {}
        if found_path:
            logger.info(f"Loading config from: {found_path}")
            with open(found_path) as f:
                yaml_data = yaml.safe_load(f) or {}
        else:
            logger.warning(
                "No config file found. Using default settings and environment variables."
            )

        return cls(**yaml_data)
