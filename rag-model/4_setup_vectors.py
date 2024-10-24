# Databricks notebook source
# MAGIC %md
# MAGIC # Setup vectors
# MAGIC This will setup 
# MAGIC  - A vector endpoint (NOTE: there is a cost per hour for running this)
# MAGIC  - The index based on the table we created in the other notebook

# COMMAND ----------

# MAGIC %pip install --quiet -U databricks-vectorsearch flashrank
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Define your attributes
catalog_name = "stu_sandbox"
schema_name = "rag_model"
vector_search_endpoint_name = "stu-test-vector"
model_endpoint_name = "bge_small_en_v1_5"

encoded_table_name = f"{catalog_name}.{schema_name}.silver_pdfs_chunked_encoded"
encoded_table_name_vs_index = f"{encoded_table_name}_vs_index"

# COMMAND ----------

#helper functions
import time

def endpoint_exists(vsc, vs_endpoint_name):
  try:
    return vs_endpoint_name in [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]
  except Exception as e:
    #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
    if "REQUEST_LIMIT_EXCEEDED" in str(e):
      print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. The demo will consider it exists")
      return True
    else:
      raise e

def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

def index_exists(vsc, endpoint_name, index_full_name):
    try:
        dict_vsindex = vsc.get_index(endpoint_name, index_full_name).describe()
        return dict_vsindex.get('status').get('ready', False)
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            print(f'Unexpected error describing the index. This could be a permission issue.')
            raise e
    return False
    
def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# COMMAND ----------

# Create the vector search endpoint if it does not exist
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if not endpoint_exists(vsc, vector_search_endpoint_name):
    vsc.create_endpoint(name=vector_search_endpoint_name, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, vector_search_endpoint_name)
print(f"Endpoint named {vector_search_endpoint_name} is ready.")

# COMMAND ----------

# Enable Change Data Feed on the Delta table
spark.sql(f"ALTER TABLE {encoded_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# Setup the vector index with data from the table
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if not index_exists(vsc, vector_search_endpoint_name, encoded_table_name_vs_index):
  print(f"Creating index {encoded_table_name_vs_index} on endpoint {vector_search_endpoint_name}...")
  vsc.create_delta_sync_index(
    endpoint_name=vector_search_endpoint_name,
    index_name=encoded_table_name_vs_index,
    source_table_name=encoded_table_name,
    pipeline_type="TRIGGERED", #Sync needs to be manually triggered
    primary_key="id",
    embedding_dimension=384, ##TODO: Get this from the model_endpoint_name
    embedding_vector_column="embedding"
  )
  #Let's wait for the index to be ready and all our embeddings to be created and indexed
  wait_for_index_to_be_ready(vsc, vector_search_endpoint_name, encoded_table_name_vs_index)
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  wait_for_index_to_be_ready(vsc, vector_search_endpoint_name, encoded_table_name_vs_index)
  vsc.get_index(vector_search_endpoint_name, encoded_table_name_vs_index).sync()

# COMMAND ----------

# Test calculate embedding for this sample query
import mlflow.deployments
from databricks.vector_search.client import VectorSearchClient
from pprint import pprint
vsc = VectorSearchClient()

question = "when is christmas shutdown?"

deploy_client = mlflow.deployments.get_deploy_client("databricks")
response = deploy_client.predict(endpoint=model_endpoint_name, inputs={"input": [question]})
embeddings = [e['embedding'] for e in response.data]

results = vsc.get_index(vector_search_endpoint_name, encoded_table_name_vs_index).similarity_search(
  query_vector=embeddings[0],
  columns=["url", "content"],
  num_results=5)

passages = []
for doc in results.get('result', {}).get('data_array', []):
    new_doc = {"file": doc[0], "text": doc[1]}
    passages.append(new_doc)
pprint(passages)

## Rerank
from flashrank import Ranker, RerankRequest
ranker = Ranker(model_name="rank-T5-flan", cache_dir="/local_disk0/cache")
rerank_request = RerankRequest(query=question, passages=passages)
results = ranker.rerank(rerank_request)
pprint(results)

