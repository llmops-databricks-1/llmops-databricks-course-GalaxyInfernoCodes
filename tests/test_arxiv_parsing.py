"""Tests for Arxiv metadata parsing module."""

from unittest.mock import MagicMock, patch

from braindrop.ingestion.arxiv_parsing import get_arxiv_metadata, parse_arxiv_id_from_text


def test_parse_arxiv_id_from_text() -> None:
    """Test parsing Arxiv ID from various strings."""
    assert parse_arxiv_id_from_text("2301.00001.pdf") == "2301.00001"
    assert parse_arxiv_id_from_text("arxiv:2301.00001v1") == "2301.00001"
    assert parse_arxiv_id_from_text("No ID here") is None


@patch("braindrop.ingestion.arxiv_parsing.arxiv.Client")
def test_get_arxiv_metadata_success(mock_client_cls: MagicMock) -> None:
    """Test successfully fetching Arxiv metadata."""
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    mock_result = MagicMock()
    mock_result.entry_id = "http://arxiv.org/abs/2301.00001v1"
    mock_result.title = "Sample Paper"
    mock_result.authors = [MagicMock(name="Author 1"), MagicMock(name="Author 2")]
    mock_result.summary = "Abstract"
    mock_result.published = MagicMock()
    mock_result.updated = MagicMock()

    mock_client.results.return_value = iter([mock_result])

    metadata = get_arxiv_metadata("2301.00001")

    assert metadata["id"] == "2301.00001"
    assert metadata["title"] == "Sample Paper"
    assert len(metadata["authors"]) == 2
