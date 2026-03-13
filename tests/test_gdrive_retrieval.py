"""Tests for Google Drive retrieval module."""

from unittest.mock import MagicMock, patch

import pytest

from braindrop.ingestion.gdrive_retrieval import GDriveClient


@pytest.fixture
def mock_gdrive_service() -> MagicMock:
    """Mock the Google Drive API service and credentials."""
    with (
        patch(
            "braindrop.ingestion.gdrive_retrieval.service_account.Credentials.from_service_account_info"
        ) as mock_creds,
        patch("braindrop.ingestion.gdrive_retrieval.build") as mock_build,
    ):
        mock_creds.return_value = MagicMock()
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        yield mock_service


def test_list_pdf_files_success(mock_gdrive_service: MagicMock) -> None:
    """Test successfully listing PDF files from a folder."""
    # Mock the list response
    mock_files = [
        {"id": "file1", "name": "paper1.pdf"},
        {"id": "file2", "name": "paper2.pdf"},
    ]

    def side_effect(q: str, **kwargs: object) -> MagicMock:
        mock_execute = MagicMock()
        if "application/pdf" in q:
            mock_execute.execute.return_value = {"files": mock_files}
        else:
            mock_execute.execute.return_value = {"files": []}
        return mock_execute

    mock_gdrive_service.files().list.side_effect = side_effect

    client = GDriveClient(service_account_info=None)
    files = client.list_pdf_files(folder_id="folder123")

    assert len(files) == 2
    assert files[0]["id"] == "file1"
    assert "folder123" in mock_gdrive_service.files().list.call_args[1]["q"]


def test_list_pdf_files_recursive(mock_gdrive_service: MagicMock) -> None:
    """Test recursively listing PDF files from subfolders."""
    # Mock behavior for the root folder
    # 1. List files (root has 1 pdf)
    # 2. List subfolders (root has 1 subfolder)

    def side_effect(q: str, **kwargs: object) -> MagicMock:
        mock_execute = MagicMock()
        if "'root_folder' in parents" in q:
            if "application/pdf" in q:
                mock_execute.execute.return_value = {
                    "files": [{"id": "pdf_root", "name": "root.pdf"}]
                }
            else:
                mock_execute.execute.return_value = {
                    "files": [{"id": "sub_folder", "name": "Subfolder"}]
                }
        elif "'sub_folder' in parents" in q:
            if "application/pdf" in q:
                mock_execute.execute.return_value = {
                    "files": [{"id": "pdf_sub", "name": "sub.pdf"}]
                }
            else:
                mock_execute.execute.return_value = {"files": []}
        return mock_execute

    mock_gdrive_service.files().list.side_effect = side_effect

    client = GDriveClient(service_account_info=None)
    files = client.list_pdf_files(folder_id="root_folder")

    assert len(files) == 2
    ids = [f["id"] for f in files]
    assert "pdf_root" in ids
    assert "pdf_sub" in ids


@patch("braindrop.ingestion.gdrive_retrieval.MediaIoBaseDownload")
def test_download_file_success(
    mock_downloader_cls: MagicMock, mock_gdrive_service: MagicMock
) -> None:
    """Test successfully downloading a file."""
    # Mock the media download
    mock_request = MagicMock()
    mock_gdrive_service.files().get_media.return_value = mock_request

    # Mock the downloader behavior
    mock_downloader = MagicMock()
    mock_downloader_cls.return_value = mock_downloader
    mock_downloader.next_chunk.return_value = (None, True)  # (progress, done)

    client = GDriveClient(service_account_info=None)
    content = client.download_file(file_id="file1")

    assert content is not None
    mock_gdrive_service.files().get_media.assert_called_once_with(fileId="file1")
    mock_downloader_cls.assert_called_once()
