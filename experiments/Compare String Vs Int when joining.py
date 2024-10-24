# Databricks notebook source
# MAGIC %md
# MAGIC # Compare String Vs Int when joining
# MAGIC This notebook creates some dummy data then compare the result of joining on String Vs Int
# MAGIC Note: The string is a hash of the int ids so the results should be identical 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the schema if it doesn't already exist
# MAGIC CREATE SCHEMA IF NOT EXISTS stu_sandbox.performance_tests;

# COMMAND ----------

# Use the desired schema for your tests
spark.sql("USE CATALOG stu_sandbox")
spark.sql("USE SCHEMA performance_tests")

# Import necessary libraries
from pyspark.sql.functions import col, lit, rand, sha2, concat_ws, concat
from pyspark.sql.types import IntegerType, StringType
import random

# Create a customer table with dummy data
def generate_customer_data():
    customer_df = (spark.range(10000000)
                   .withColumn("id", col("id").cast(IntegerType()))
                   .withColumn("name", sha2(col("id").cast(StringType()), 256).substr(0, 10))
                   .withColumn("address", sha2(col("id").cast(StringType()), 256).substr(0, 20))
                   .withColumn("city", concat(lit("City_"), sha2(col("id").cast(StringType()), 256).substr(0, 5)))
                   .withColumn("phone_number", sha2(col("id").cast(StringType()), 256).substr(0, 10))
                   .withColumn("product_int_id", (rand() * 10000).cast(IntegerType())) # random int between 1 and 10,000
                   .withColumn("product_string_id", sha2(col("product_int_id").cast(StringType()), 256).substr(0, 10)) # hashed product_int_id
                  )
    return customer_df

customer_df = generate_customer_data()
customer_df.write.saveAsTable("customer")

# Create a product table with dummy data
def generate_product_data():
    product_df = (spark.range(1000000)
                  .withColumn("product_int_id", col("id").cast(IntegerType()))
                  .withColumn("product_string_id", sha2(col("product_int_id").cast(StringType()), 256).substr(0, 10)) # hashed product_string_id
                  .withColumn("product_name", sha2(col("id").cast(StringType()), 256).substr(0, 15))
                  .withColumn("price", (rand() * 1000).cast("decimal(10,2)"))
                  .withColumn("product_code", sha2(col("id").cast(StringType()), 256).substr(0, 8))
                 )
    return product_df

product_df = generate_product_data()
product_df.write.saveAsTable("product")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from stu_sandbox.performance_tests.customer c
# MAGIC inner join stu_sandbox.performance_tests.product p
# MAGIC on c.product_int_id = p.product_int_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Do the comparison

# COMMAND ----------

import time
import statistics

# Disable Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "false")
# Disable broadcast joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

def clear_cache():
    spark.catalog.clearCache()

# Repeat the experiment 10 times for both joins
int_durations = []
string_durations = []

for i in range(10):
    # Clear cache before each run
    clear_cache()

    # Read from the persisted tables
    customer_df = spark.table("stu_sandbox.performance_tests.customer")
    product_df = spark.table("stu_sandbox.performance_tests.product")
    
    # Join on product_int_id
    start_time = time.time()
    int_join_df = customer_df.join(product_df, on="product_int_id")
    int_join_df.write.format("noop").mode("overwrite").save() # trigger the job
    int_duration = (time.time() - start_time) * 1000  # convert to milliseconds
    int_durations.append(int_duration)
    
    # Clear cache before the next join
    clear_cache()

    # Join on product_string_id
    start_time = time.time()
    string_join_df = customer_df.join(product_df, on="product_string_id")
    string_join_df.write.format("noop").mode("overwrite").save() # trigger the job
    string_duration = (time.time() - start_time) * 1000  # convert to milliseconds
    string_durations.append(string_duration)

# Calculate the average and standard deviation for both joins
int_avg_duration = statistics.mean(int_durations)
int_std_duration = statistics.stdev(int_durations)

string_avg_duration = statistics.mean(string_durations)
string_std_duration = statistics.stdev(string_durations)

# Show results
print(f"Join on integer ID took an average of {int_avg_duration:.2f} milliseconds "
      f"(± {int_std_duration:.2f} ms over 10 runs)")
print(f"Join on string ID took an average of {string_avg_duration:.2f} milliseconds "
      f"(± {string_std_duration:.2f} ms over 10 runs)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the schema if it doesn't already exist
# MAGIC DROP TABLE  stu_sandbox.performance_tests.customer;
# MAGIC DROP TABLE  stu_sandbox.performance_tests.product;
# MAGIC DROP SCHEMA stu_sandbox.performance_tests;
