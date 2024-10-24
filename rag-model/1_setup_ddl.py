# Databricks notebook source
# MAGIC %md
# MAGIC # Setup DDL
# MAGIC This creates the schema, catalog and managed volume for the pdf data and also the checkpoints

# COMMAND ----------

# Define variables
catalog_name = 'stu_sandbox'
schema_name = 'rag_model'

# Create the catalog
spark.sql(f"""
    create catalog if not exists {catalog_name}
""")

# Use the newly created catalog
spark.sql(f"""
    use catalog {catalog_name}
""")

# Create the schema
spark.sql(f"""
    create schema if not exists {schema_name}
""")

# Create managed volume for pdf data
spark.sql(f"""
    create volume if not exists {catalog_name}.{schema_name}.pdf_data
""")

# Create managed volume for checkpoints
spark.sql(f"""
    create volume if not exists {catalog_name}.{schema_name}.checkpoints
""")

