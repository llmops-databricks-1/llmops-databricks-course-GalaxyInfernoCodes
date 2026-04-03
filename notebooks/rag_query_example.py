# Databricks notebook source
# MAGIC %md
# MAGIC # Braindrop RAG Query Example
# MAGIC This notebook demonstrates how to query the Vector Search index we created
# MAGIC and use the results to answer a question using a Databricks Foundation Model.

# COMMAND ----------


from databricks.sdk import WorkspaceClient, dbutils
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from pyspark.sql import SparkSession

from braindrop.config import ProjectConfig
from braindrop.embedding.vector_search_manager import VectorSearchManager


def get_spark() -> SparkSession:
    """Get or create a Spark session."""
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


spark = get_spark()

# 1. Initialize Configuration and Managers
config = ProjectConfig.load()
vs_manager = VectorSearchManager(spark, config)
w = WorkspaceClient()

# 2. Define your question
question = (
    "What is most important lesson when learning how to build applications with GenAI?"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Retrieval
# MAGIC Search the Vector Index for the most relevant text chunks.

# COMMAND ----------

print(f"Searching for: {question}...")

# vs_manager.search returns results with columns: [id, text, authors, title]
search_results = vs_manager.search(query=question, num_results=5)

# Extract and format the retrieved context
data_array = search_results.get("result", {}).get("data_array", [])

if not data_array:
    print("No relevant chunks found. Is the index sync finished?")
    dbutils.notebook.exit("No results")

chunks = [row[1] for row in data_array]
context_text = "\n\n---\n\n".join(chunks)

print(f"Retrieved {len(chunks)} relevant chunks.")

# Shows chunks
print(context_text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generation
# MAGIC Send the question and the retrieved context to a Foundation Model (LLM).

# COMMAND ----------

# Construct the RAG prompt
prompt = f"""You are a helpful research assistant.
Answer the user's question using ONLY the provided context.
If the answer is not in the context, explain your best guess based on provided context.

CONTEXT:
{context_text}

USER QUESTION:
{question}
"""

# Query the LLM (DBRX is highly capable for research tasks)
llm_model = "databricks-llama-4-maverick"

print(f"Querying LLM ({llm_model})...")

try:
    response = w.serving_endpoints.query(
        name=llm_model,
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
    )

    print("\n" + "=" * 20 + " LLM ANSWER " + "=" * 20 + "\n")
    print(response.choices[0].message.content)
    print("\n" + "=" * 52)

except Exception as e:
    print(f"Error querying LLM: {e}")
    print("Hint: Ensure the serving endpoint is active in your workspace.")

# COMMAND ----------
