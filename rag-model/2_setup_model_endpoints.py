# Databricks notebook source
# MAGIC %md
# MAGIC # Create model endpoints
# MAGIC This sets up the model serving endpoint for the model that is going to be used.  You could do this step in the UI if you prefer:
# MAGIC
# MAGIC - Access the Serving UI:
# MAGIC   - Click on Serving in the Databricks sidebar to display the Serving UI.
# MAGIC - Create a Serving Endpoint:
# MAGIC   - Click Create serving endpoint.
# MAGIC - Configure the Endpoint:
# MAGIC   - name: gte-large-en
# MAGIC   - model: system.ai.bge_small_en_v1_5
# MAGIC   - version: 1

# COMMAND ----------

# Define the endpoint name and configuration
from collections import namedtuple

# ModelConfig
ModelConfig = namedtuple("ModelConfig", ["endpoint_name", "feature_spec_name", "entity_version"])
embedding_model = ModelConfig("bge_small_en_v1_5", "system.ai.bge_small_en_v1_5", 1)
chat_model = ModelConfig("dbrx_instruct", "system.ai.dbrx_instruct", 3)

# List of models
models = [embedding_model, chat_model]



# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

# Initialize the workspace client
workspace = WorkspaceClient()

# Iterate through each model to create endpoints
for model in models:
    # Check if the endpoint already exists
    try:
        status = workspace.serving_endpoints.get(name=model.endpoint_name)
        print(f"Endpoint {model.endpoint_name} already exists.")
    except Exception as e:
        if "does not exist" in str(e):
            # Create the endpoint if it does not exist
            status = workspace.serving_endpoints.create_and_wait(
                name=model.endpoint_name,
                config=EndpointCoreConfigInput(
                    served_entities=[
                        ServedEntityInput(
                            entity_name=model.feature_spec_name,
                            entity_version=model.entity_version,
                            scale_to_zero_enabled=True,
                            workload_size="Small"
                        )
                    ]
                )
            )
            print(f"Created endpoint {model.endpoint_name}.")
        else:
            raise e

    # Print the status of the endpoint
    print(status)
    print("end")
