# Track: Preprocess Arxiv Papers from Google Drive

## Overview
This track focuses on the initial data ingestion and preprocessing pipeline. It involves querying PDF files from a Google Drive folder, extracting text and metadata from Arxiv papers, and storing the results in Databricks for subsequent RAG indexing.

## Scope & Goals
- **Google Drive Integration:** Authenticate and query PDF files from a specified folder.
- **PDF Preprocessing:** Extract clean text from Arxiv PDFs.
- **Metadata Extraction:** Parse Arxiv-specific metadata (title, authors, publication date, etc.).
- **Databricks Storage:** Save extracted metadata to a Delta table and store text in a format ready for Vector Search indexing.

## Functional Requirements
- Securely access Google Drive via API or service account.
- Handle PDF text extraction reliably, including handling multiple columns if necessary.
- Parse and structure metadata for consistency.
- Ensure all data is managed within Databricks Unity Catalog.

## Technical Constraints
- Use `uv` for dependency management.
- Utilize Databricks Connect for local-to-cloud development.
- Adhere to the project's code style guidelines and >40% test coverage.
- All deployments must be via Databricks Asset Bundles (DABs).

## Non-Functional Requirements
- **Logging:** Comprehensive logging of the ingestion and preprocessing steps.
- **Error Handling:** Robust handling of failed downloads or parsing errors.
- **Efficiency:** Parallel processing for faster PDF extraction if required.
