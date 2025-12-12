# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://retail@retailprojectstorage01.blob.core.windows.net",
  mount_point = "/mnt/retail_projects",
  extra_configs = {"fs.azure.account.key.retailprojectstorage01.blob.core.windows.net":"C9/uNN8FIczxKJq1bb2UI1qelchAcVJUNEvfp4VYP19c5SnmDR/7gwsrsnCKnJgOVBqaU1r8s2G0+ASt3TvDuw=="})


# COMMAND ----------

dbutils.fs.ls('/mnt/retail_projects/bronze/')

# COMMAND ----------

# Read raw data from Bronze layer

df_transactions = spark.read.parquet('/mnt/retail_projects/bronze/transaction/')
df_products = spark.read.parquet('/mnt/retail_projects/bronze/product/')
df_stores = spark.read.parquet('/mnt/retail_projects/bronze/store/')

df_customers = spark.read.parquet('/mnt/retail_projects/bronze/customer/manish040596/azure-data-engineer---multi-source/refs/heads/main/')

df1 = df_stores.filter(df_stores.store_id > 1)

display(df1)

# COMMAND ----------

display(df_transactions);


# COMMAND ----------

# DBTITLE 1,create silver layer - data cleaning
from pyspark.sql.functions import col

# Convert types and clean data
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


# COMMAND ----------

# DBTITLE 1,Join all data together
from pyspark.sql.functions import col

df_silver = df_transactions \
    .join(df_customers, "customer_id") \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))


# COMMAND ----------

display(df_silver)

# COMMAND ----------

# DBTITLE 1,dump to adls location
silver_path = "/mnt/retail_projects/silver/"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

display(df_silver)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS retail_silver_cleaned")
spark.sql(
    """
    CREATE TABLE retail_silver_cleaned
    USING DELTA
    LOCATION '/mnt/retail_projects/silver/'
    """
)

# COMMAND ----------

# MAGIC %sql select * from retail_silver_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold layer
 # Load cleaned transaction from Silver layer

 silver_df = spark.read.format("delta").load("/mnt/retail_projects/silver/")


# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "transaction_date", "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

# COMMAND ----------

display(gold_df);

# COMMAND ----------

gold_path = "/mnt/retail_projects/gold/"

gold_df.write.mode("overwrite").format("delta").save(gold_path)


# COMMAND ----------

spark.sql("""
CREATE TABLE reatil_gold_sales_summary
USING DELTA
LOCATION '/mnt/retail_projects/gold/'
""")

# COMMAND ----------

# MAGIC %sql select * from reatil_gold_sales_summary;

# COMMAND ----------

 