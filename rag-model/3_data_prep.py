# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC This notebook is setup to 
# MAGIC  - Ingest PDF binary data to a table from a managed storage location
# MAGIC  - It then uses a chunk function (read_as_chunk) to tokenise the data
# MAGIC

# COMMAND ----------

# MAGIC %pip install --quiet -U transformers==4.41.1 langchain-text-splitters==0.2.0 databricks-vectorsearch mlflow tiktoken==0.7.0 torch==2.3.0 llama-index==0.10.43
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Define your attributes
username = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

catalog_name = "stu_sandbox"
schema_name = "rag_model"
volume_name = "pdf_data"
model_endpoint_name = "bge_small_en_v1_5"
articles_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

raw_table_name = f"{catalog_name}.{schema_name}.bronze_pdfs_raw"
raw_checkpoints_path = f"/Volumes/{catalog_name}/{schema_name}/checkpoints/bronze_pdfs_raw/"

encoded_table_name = f"{catalog_name}.{schema_name}.silver_pdfs_chunked_encoded"
encoded_checkpoints_path = f"/Volumes/{catalog_name}/{schema_name}/checkpoints/silver_pdfs_chunked_encoded/"

print(f"Username:                 {username}")
print(f"catalog_name:             {catalog_name}")
print(f"schema_name:              {schema_name}")
print(f"volume_name:              {volume_name}")
print(f"articles_path:            {articles_path}")
print(f"raw_table_name:           {raw_table_name}")
print(f"raw_checkpoints_path:     {raw_checkpoints_path}")
print(f"encoded_table_name:       {encoded_table_name}")
print(f"encoded_checkpoints_path: {encoded_checkpoints_path}")


# COMMAND ----------

# (optional) test model endpoint is working
from mlflow.deployments import get_deploy_client
from pprint import pprint

# gte-large-en Foundation models are available using the /serving-endpoints/databricks-gtegte-large-en/invocations api. 
deploy_client = get_deploy_client("databricks")

## NOTE: if you change your embedding model here, make sure you change it in the query step too
embeddings = deploy_client.predict(endpoint=model_endpoint_name, inputs={"input": ["What is Apache Spark?"]})
pprint(embeddings)

# COMMAND ----------

# Set the Arrow batch size (NB this won't work on serverless - obvs!)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10)

# COMMAND ----------

# Helper Functions

import warnings
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core import Document, set_global_tokenizer
from transformers import AutoTokenizer
from typing import Iterator
from pyspark.sql.functions import pandas_udf
import pandas as pd
import io

#Convert binary pdf to text
#def parse_bytes_pypdf(raw_doc_contents_bytes: bytes):
#    try:
#        pdf = io.BytesIO(raw_doc_contents_bytes)
#        reader = PdfReader(pdf)
#        parsed_content = [page_content.extract_text() for page_content in reader.pages]
#        return "\n".join(parsed_content)
#    except Exception as e:
#        warnings.warn(f"Exception {e} has been thrown during parsing")
#        return None
      
def parse_bytes_markdown(raw_doc_contents_bytes: bytes) -> str:
    try:
        # Decode bytes and convert Markdown to plain text
        markdown_content = raw_doc_contents_bytes.decode('utf-8')
        return markdown_content
    except Exception as e:
        warnings.warn(f"Exception {e} has been thrown during parsing")
        return None
    
#Convert split test into chunks.  NOTE: There are a millon ways to do this depending on your txt
@pandas_udf("array<string>")
def read_as_chunk(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    set_global_tokenizer(
      AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")
    )
    splitter = SentenceSplitter(chunk_size=500, chunk_overlap=10)
    def extract_and_split(b):
      txt = parse_bytes_markdown(b)
      if txt is None:
        return []
      nodes = splitter.get_nodes_from_documents([Document(text=txt)])
      return [n.text for n in nodes]

    for x in batch_iter:
        yield x.apply(extract_and_split)

#Convert Convert chunk into vector(s)
@pandas_udf("array<float>")
def get_embedding(contents: pd.Series) -> pd.Series:
    import mlflow.deployments
    deploy_client = mlflow.deployments.get_deploy_client("databricks")
    def get_embeddings(batch):
        #Note: this will fail if an exception is thrown during embedding creation (add try/except if needed) 
        response = deploy_client.predict(endpoint=model_endpoint_name, inputs={"input": batch})
        return [e['embedding'] for e in response.data]

    # Splitting the contents into batches of 150 items each, since the embedding model takes at most 150 inputs per request.
    max_batch_size = 150
    batches = [contents.iloc[i:i + max_batch_size] for i in range(0, len(contents), max_batch_size)]

    # Process each batch and collect the results
    all_embeddings = []
    for batch in batches:
        all_embeddings += get_embeddings(batch.tolist())

    return pd.Series(all_embeddings)

# COMMAND ----------

# Read data into table
df = (spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'BINARYFILE')
        .option("pathGlobFilter", "*.md")
        .load(articles_path))

# Write the data as a Delta table
(df.writeStream
  .trigger(availableNow=True)
  .option("checkpointLocation", raw_checkpoints_path)
  .table(raw_table_name).awaitTermination())

# COMMAND ----------

from pyspark.sql.functions import explode, concat, col, lit
import uuid

# Function to generate UUID
def generate_uuid():
    return str(uuid.uuid4())

# Register the function as a UDF
uuid_udf = spark.udf.register("uuid_udf", generate_uuid)

# Write the embeddings as a Delta table
(spark.readStream
    .option("ignoreDeletes", "true")
    .option("ignoreChanges", "true")
    .table(raw_table_name)
    .withColumn("content", explode(read_as_chunk("content")))
    .withColumn("embedding", get_embedding("content"))
    .withColumn("id", uuid_udf())
    .selectExpr('path as url', 'content', 'embedding', 'id')
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", encoded_checkpoints_path)
    .option("mergeSchema", "true")
    .table(encoded_table_name)
    .awaitTermination())

# COMMAND ----------

df = spark.sql(f"SELECT * FROM {encoded_table_name}")
display(df)
