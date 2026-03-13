# Tech Stack: Braindrop (LLMOps on Databricks)

## Core Platform & Infrastructure
- **Cloud Platform:** Databricks
- **Compute:** Databricks Serverless
- **Infrastructure as Code:** Databricks Asset Bundles (DABs)
- **Local Connectivity:** Databricks Connect

## Programming Language & Environment
- **Language:** Python 3.12
- **Package Management:** UV, Setuptools
- **Local Development:** Visual Studio Code (VScode) with Databricks Extension

## AI/ML Frameworks
- **Agent Framework:** Databricks Agents (`databricks-agents`)
- **Lifecycle Management:** MLflow (Tracking, Models, Registry)
- **Vector Search:** Databricks Vector Search
- **LLM Connectivity:** Databricks SDK, OpenAI SDK

## Data Storage & Management
- **Primary Storage:** Delta Lake (Delta Tables) on Unity Catalog
- **Data Access:** SQL via Databricks Connect and Databricks SDK

## Development & Quality Standards
- **Testing:** Pytest
- **Linting & Formatting:** Ruff (Enforced in `pyproject.toml`)
- **Version Control:** Git
- **Pre-commit Hooks:** Enforced via `.pre-commit-config.yaml`
- **CI/CD:** GitHub Actions integrated with Databricks Asset Bundles
