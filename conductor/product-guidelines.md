# Product Guidelines: Braindrop (LLMOps on Databricks)

## Prose Style
Documentation and communications should follow a **Technical & Concise** style.
- **Precision:** Use accurate technical terminology (e.g., "Delta table", "RAG agent").
- **Minimalism:** Avoid unnecessary filler words; focus on actionable information.
- **Clarity:** Ensure complex concepts are explained simply but without compromising technical depth.

## Branding & Terminology
Adhere to a hybrid of **Databricks Native** and **LLMOps Standard** terminology.
- **Databricks Specifics:** Refer to core platform components correctly (e.g., Asset Bundles, Serverless Compute, Connect, Vector Search).
- **LLMOps Standards:** Use industry-standard terms for AI components (e.g., RAG, Retrieval, Agents, Prompt Engineering, Evaluation).

## Developer Experience (UX) Principles
Prioritize a **CLI & Automation Focus** and **API Design & Readability**.
- **Automation First:** Workflows should be driven by CLI tools (UV, DABs) and configuration-driven deployments.
- **Readability:** Maintain high standards for API designs, ensuring that function signatures are intuitive and well-documented.
- **Scalability:** Design modules that are reusable and easy to test across different environments.

## Quality Standards
Enforce high standards through **Strict Linting & Formatting** and **Automated Quality Checks**.
- **Linting & Formatting:** Adhere strictly to the rules defined in `pyproject.toml` using Ruff.
- **Automated Checks:** All code must pass automated linting, formatting, and test suites before deployment.
- **Verification:** Ensure all deployments are verifiable and consistent using Databricks Asset Bundles.
