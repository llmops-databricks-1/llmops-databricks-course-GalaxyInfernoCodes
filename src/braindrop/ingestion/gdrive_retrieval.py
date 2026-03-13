"""Google Drive retrieval module for PDFs."""

import io
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
