from unittest.mock import MagicMock, mock_open, patch

from braindrop.ingestion.volume_storage import save_to_volume


def test_save_to_volume() -> None:
    mock_spark = MagicMock()
    content = b"pdf content"
    catalog = "dev"
    schema = "braindrop"
    volume = "arxiv_pdfs"
    filename = "test.pdf"

    # Mocking open and os.makedirs to prevent actual file creation
    with (
        patch("braindrop.ingestion.volume_storage.open", mock_open()) as mocked_file,
        patch("braindrop.ingestion.volume_storage.os.makedirs") as mocked_makedirs,
    ):
        path = save_to_volume(mock_spark, content, catalog, schema, volume, filename)

        expected_path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"
        assert path == expected_path
        mocked_makedirs.assert_called_once()
        mocked_file.assert_called_once_with(expected_path, "wb")
        mocked_file().write.assert_called_once_with(content)
