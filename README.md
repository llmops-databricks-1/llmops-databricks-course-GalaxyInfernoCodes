<h1 align="center">
LLMOps Course on Databricks: Braindrop
</h1>

## Project Overview
Braindrop is a modular RAG (Retrieval-Augmented Generation) pipeline built on Databricks. It automates the transition from unstructured PDF documents in Google Drive to a structured, searchable knowledge base for LLMs.

### The 3-Stage Pipeline
The data flow is managed through Databricks Asset Bundles (DABs) and organized into three sequential jobs:

1.  **01 Ingestion (`task download-pdfs`):** Downloads PDFs from Google Drive to a Databricks Volume and registers them in the `source_pdfs` table.
2.  **02 Extraction (`task parse-pdfs`):** Uses Databricks `ai_parse_document` to convert binary PDFs into structured JSON/Markdown.
3.  **03 Indexing (`task create-embeddings`):** Chunks and cleans the parsed text, then synchronizes it with a Databricks Vector Search index for semantic retrieval.

## Setup & Deployment

### Prerequisites
- **UV:** Used for dependency management. Install via [astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/).
- **Databricks CLI:** Configured with a profile (e.g., `sg-free` or `cauchy`).
- **Secrets:** A Databricks Secret Scope named `llmops-project` must contain a `drive-api-key` (Google Service Account JSON) to access the PDF source.
    - You can use `task setup-secrets` to automate this if your `.env` is configured.

### Environment Setup
Create a local virtual environment and install dependencies:
```bash
uv sync --extra dev
```

### Authentication & Secrets
Ensure you are logged into your workspace and your secrets are uploaded:
```bash
task auth
task setup-secrets
```

We need credentials to access the Google Drive folder containing the PDFs. These credentials are stored in a Databricks Secret Scope named `sarah-llmops-project` under the key `drive-api-key`. The download-job then uses these credentials to download the PDFs to a Databricks Volume.

### Common Commands (Taskfile)
I have designed and deployed 3 jobs to Databricks:
1. download-pdfs: Downloads PDFs from Google Drive to a Databricks Volume and registers them in the `source_pdfs` table.
2. parse-pdfs: Uses Databricks `ai_parse_document` to convert binary PDFs into structured JSON/Markdown.
3. create-embeddings: Chunks and cleans the parsed text, then synchronizes it with a Databricks Vector Search index for semantic retrieval.

I have also created a Taskfile to automate the deployment and triggering of these jobs. All jobs are designed as scripts and deployed via Databricks Asset Bundles.

| Command | Description |
| :--- | :--- |
| `task deploy` | Deploys the Databricks Asset Bundle to the workspace. |
| `task download-pdfs` | Triggers the GDrive ingestion job. |
| `task parse-pdfs` | Triggers the AI document parsing job. |
| `task create-embeddings` | Processes chunks and syncs the Vector Search index. |
| `task check-auth` | Verifies your current Databricks CLI connection. |

## Querying the RAG System
Once the pipeline has finished and the Vector Search index is "Online," you can use the example notebook in `notebooks/rag_query_example.py` to ask questions against your PDF library using Databricks Foundation Models (like DBRX).

---
*Developed as part of the LLMOps Databricks Course.*
