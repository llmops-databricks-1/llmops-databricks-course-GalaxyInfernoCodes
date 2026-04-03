"""PDF text extraction module using docling."""

import io

from docling.datamodel.base_models import DocumentStream
from docling.document_converter import DocumentConverter


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF bytes and return it in markdown format."""
    converter = DocumentConverter()

    stream = DocumentStream(name="input.pdf", stream=io.BytesIO(pdf_content))

    result = converter.convert(stream)

    return result.document.export_to_markdown()
