# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup
# MAGIC Run this to clean up everthing that was created

# COMMAND ----------

# MAGIC %pip install --quiet -U databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

catalog_name = "stu_sandbox"
schema_name = "rag_model"
volume_name = "pdf_data"
model_endpoint_name = "bge_small_en_v1_5"
articles_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

raw_table_name = f"{catalog_name}.{schema_name}.bronze_pdfs_raw"
raw_checkpoints_path = f"/Volumes/{catalog_name}/{schema_name}/checkpoints/bronze_pdfs_raw/"

encoded_table_name = f"{catalog_name}.{schema_name}.silver_pdfs_chunked_encoded"
encoded_checkpoints_path = f"/Volumes/{catalog_name}/{schema_name}/checkpoints/silver_pdfs_chunked_encoded/"

vector_search_endpoint_name = "stu-test-vector"

encoded_table_name = f"{catalog_name}.{schema_name}.silver_pdfs_chunked_encoded"
encoded_table_name_vs_index = f"{encoded_table_name}_vs_index"

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

vsc.delete_index(endpoint_name=vector_search_endpoint_name, index_name=encoded_table_name_vs_index)
vsc.delete_endpoint(name=vector_search_endpoint_name)



# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

# Drop model serving endpoint
workspace = WorkspaceClient()

workspace.serving_endpoints.delete(model_endpoint_name)

# COMMAND ----------



# COMMAND ----------

spark.sql(f"""
    use catalog {catalog_name}
""")

# Create managed volume for pdf data
spark.sql(f"""
    delete volume if exists {catalog_name}.{schema_name}.pdf_data
""")

# Create managed volume for checkpoints
spark.sql(f"""
    delete volume if exists {catalog_name}.{schema_name}.checkpoints
""")

# Create the schema
spark.sql(f"""
    delete schema if exists {schema_name}
""")

# Create the catalog
spark.sql(f"""
    delete catalog if exists {catalog_name}
""")
