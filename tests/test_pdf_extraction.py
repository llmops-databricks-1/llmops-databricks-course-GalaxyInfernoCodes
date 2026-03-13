"""Tests for PDF extraction module."""

from unittest.mock import MagicMock, patch

from braindrop.ingestion.pdf_extraction import extract_text_from_pdf


@patch("braindrop.ingestion.pdf_extraction.DocumentConverter")
def test_extract_text_from_pdf_success(mock_converter_cls: MagicMock) -> None:
    """Test successfully extracting text from a PDF."""
    mock_converter = MagicMock()
    mock_converter_cls.return_value = mock_converter

    mock_result = MagicMock()
    mock_document = MagicMock()
    mock_document.export_to_markdown.return_value = "Extracted Text"
    mock_result.document = mock_document
    mock_converter.convert.return_value = mock_result

    pdf_content = b"%PDF-1.4 mock content"
    text = extract_text_from_pdf(pdf_content)

    assert text == "Extracted Text"
    mock_converter.convert.assert_called_once()
