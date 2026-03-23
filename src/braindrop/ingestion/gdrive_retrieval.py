"""Google Drive retrieval module for PDFs."""

import io
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


class GDriveClient:
    """Client for interacting with Google Drive API."""

    def __init__(self, service_account_info: dict[str, Any] | None = None) -> None:
        """Initialize GDriveClient.

        If service_account_info is provided, use it.
        Otherwise, fall back to default credentials.
        """
        if service_account_info:
            self.creds = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/drive.readonly"],
            )
        else:
            # This will use GOOGLE_APPLICATION_CREDENTIALS if set
            import google.auth

            self.creds, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/drive.readonly"]
            )

        self.service = build("drive", "v3", credentials=self.creds)

    def list_pdf_files(self, folder_id: str) -> list[dict[str, str]]:
        """List PDF files in a specific Google Drive folder and its subfolders."""
        all_files = []

        # 1. List PDF files in the current folder
        query = (
            f"'{folder_id}' in parents and "
            "mimeType = 'application/pdf' and trashed = false"
        )
        results = (
            self.service.files()
            .list(q=query, fields="nextPageToken, files(id, name)")
            .execute()
        )
        all_files.extend(results.get("files", []))

        # 2. List subfolders and recurse
        folder_query = (
            f"'{folder_id}' in parents and "
            "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
        folder_results = (
            self.service.files()
            .list(q=folder_query, fields="nextPageToken, files(id, name)")
            .execute()
        )

        for subfolder in folder_results.get("files", []):
            all_files.extend(self.list_pdf_files(subfolder["id"]))

        return all_files

    def download_file(self, file_id: str) -> bytes | None:
        """Download a file from Google Drive."""
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        return fh.getvalue()


def download_pdfs_to_local(
    folder_id: str, local_dir: Path, client: GDriveClient | None = None
) -> list[Path]:
    """Download all PDFs from a GDrive folder to a local directory.

    Returns a list of local paths to the downloaded files.
    """
    if client is None:
        client = GDriveClient()

    local_dir.mkdir(parents=True, exist_ok=True)
    files = client.list_pdf_files(folder_id)
    downloaded_paths = []

    for f in files:
        filename = f["name"]
        file_id = f["id"]
        local_path = local_dir / filename

        print(f"Downloading: {filename}...")
        try:
            content = client.download_file(file_id)
            if content:
                with open(local_path, "wb") as pdf_file:
                    pdf_file.write(content)
                downloaded_paths.append(local_path)
                print(f"Saved to: {local_path}")
            else:
                print(f"Failed to download {filename}")
        except Exception as e:
            print(f"Error downloading {filename}: {e}")

    return downloaded_paths


def download_pdfs_to_volume(
    folder_id: str,
    catalog: str,
    schema: str,
    volume: str,
    spark: Any = None,  # noqa: ANN401
    client: GDriveClient | None = None,
) -> list[str]:
    """Download all PDFs from a GDrive folder directly to a Databricks Volume.

    Returns a list of Volume paths to the downloaded files.
    """
    from braindrop.ingestion.volume_storage import save_to_volume

    if client is None:
        client = GDriveClient()

    files = client.list_pdf_files(folder_id)
    saved_paths = []

    for f in files:
        filename = f["name"]
        file_id = f["id"]

        print(f"Downloading {filename} to Volume...")
        try:
            content = client.download_file(file_id)
            if content:
                path = save_to_volume(spark, content, catalog, schema, volume, filename)
                saved_paths.append(path)
                print(f"Saved to Volume: {path}")
            else:
                print(f"Failed to download {filename}")
        except Exception as e:
            print(f"Error downloading {filename} to volume: {e}")

    return saved_paths
