"""Module for storing PDFs into Databricks Volumes."""

import io
import os
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient


def ensure_volume_exists(
    spark: Any,  # noqa: ANN401
    catalog: str,
    schema: str,
    volume: str,
) -> None:
    """Ensure the schema and volume exist in Databricks."""
    print(f"Ensuring volume '{catalog}.{schema}.{volume}' exists...")

    # 1. Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # 2. Create volume if it doesn't exist
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
    print(f"Volume {catalog}.{schema}.{volume} is ready.")


def save_to_volume(
    spark: Any,  # noqa: ANN401
    content: bytes,
    catalog: str,
    schema: str,
    volume: str,
    filename: str,
) -> str:
    """Save binary content to a Databricks Volume.

    Returns the final path of the saved file.
    """
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"

    # For Databricks Volumes, standard file I/O is preferred on clusters
    # because /Volumes is mounted via FUSE.
    try:
        os.makedirs(os.path.dirname(volume_path), exist_ok=True)
        with open(volume_path, "wb") as f:
            f.write(content)
        return volume_path
    except Exception as e:
        # If standard I/O fails (e.g., running locally without FUSE), use WorkspaceClient
        print(f"Standard I/O failed for {volume_path}: {e}. Trying WorkspaceClient...")
        try:
            w = WorkspaceClient()
            w.files.upload(volume_path, io.BytesIO(content), overwrite=True)
            return volume_path
        except Exception as sdk_e:
            print(f"WorkspaceClient upload failed: {sdk_e}")
            raise OSError(f"Failed to save {filename} to volume {volume_path}") from sdk_e


def upload_local_file_to_volume(
    spark: Any,  # noqa: ANN401
    local_path: Path,
    catalog: str,
    schema: str,
    volume: str,
) -> str:
    """Upload a local file to a Databricks Volume using WorkspaceClient."""
    destination_path = f"/Volumes/{catalog}/{schema}/{volume}/{local_path.name}"

    print(f"Uploading {local_path} to {destination_path}...")

    try:
        # Get file size for logging
        file_size = os.path.getsize(local_path)
        file_size_mb = file_size / (1024 * 1024)
        print(f"File size: {file_size_mb:.2f} MB")

        with open(local_path, "rb") as f:
            content = f.read()

        return save_to_volume(spark, content, catalog, schema, volume, local_path.name)
    except Exception as e:
        print(f"Failed to upload local file: {e}")
        raise
