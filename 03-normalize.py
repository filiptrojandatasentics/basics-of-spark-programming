# Databricks notebook source
# MAGIC %md
# MAGIC Part of the [Data scientist track](https://www.notion.so/datasentics/Basics-of-Spark-programming-1de1e938f85e461292385b31a556c80f?pvs=4)

# COMMAND ----------

# MAGIC %run ./Setup-Exercise-03

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.A - Create &amp; Use Database</h2>
# MAGIC
# MAGIC By using a specific database, we can avoid contention to commonly named tables that may be in use by other users of the workspace.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Create the database identified by the variable **`user_db`**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database
# MAGIC
# MAGIC **Special Notes**
# MAGIC * Do not hard-code the database name - in some scenarios this will result in validation errors.
# MAGIC * For assistence with the SQL command to create a database, see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-database.html" target="_blank">CREATE DATABASE</a> on the Databricks docs website.
# MAGIC * For assistence with the SQL command to use a database, see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-usedb.html" target="_blank">USE DATABASE</a> on the Databricks docs website.

# COMMAND ----------

user_db

# COMMAND ----------

spark.sql(f"create database if not exists {user_db}")

# COMMAND ----------

spark.sql(f"use {user_db}")

# COMMAND ----------

reality_check_03_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.B - Load &amp; Cache Batch Orders</h2>
# MAGIC
# MAGIC Next, we need to load the batch orders from the previous exercise and then cache them in preparation to transform the data later in this exercise.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the delta dataset we created in the previous exercise, identified by the variable **`batch_source_path`**.
# MAGIC * Using that same dataset, create a temporary view identified by the variable **`batch_temp_view`**.
# MAGIC * Cache the temporary view.

# COMMAND ----------

batch_temp_view

# COMMAND ----------

df = spark.read.parquet(batch_source_path)
df.count()

# COMMAND ----------

df.createOrReplaceTempView(batch_temp_view)

# COMMAND ----------

res = spark.sql(f"select count(1) from {batch_temp_view}")
res.show()

# COMMAND ----------

spark.catalog.cacheTable(batch_temp_view) 

# COMMAND ----------

reality_check_03_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.C - Extract Sales Reps</h2>
# MAGIC
# MAGIC Our batched orders from Exercise #2 contains thousands of orders and with every order, is the name, SSN, address and other information on the sales rep making the order.
# MAGIC
# MAGIC We can use this data to create a table of just our sales reps.
# MAGIC
# MAGIC If you consider that we have only ~100 sales reps, but thousands of orders, we are going to have a lot of duplicate data in this space.
# MAGIC
# MAGIC Also unique to this set of data, is the fact that social security numbers were not always sanitized meaning sometime they were formatted with hyphens and in other cases they were not - this is something we will have to address here.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * The SSN numbers have errors in them that we want to track - add the **`boolean`** column **`_error_ssn_format`** - for any case where **`sales_rep_ssn`** has a hypen in it, set this value to **`true`** otherwise **`false`**
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`sales_rep_ssn`** should be represented as a **`Long`** (Note: You will have to first clean the column by removing extreneous hyphens in some records)
# MAGIC   * The column **`sales_rep_zip`** should be represented as an **`Integer`**
# MAGIC * Remove the columns not directly related to the sales-rep record:
# MAGIC   * Unrelated ID columns: **`submitted_at`**, **`order_id`**, **`customer_id`**
# MAGIC   * Shipping address columns: **`shipping_address_attention`**, **`shipping_address_address`**, **`shipping_address_city`**, **`shipping_address_state`**, **`shipping_address_zip`**
# MAGIC   * Product columns: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * Because there is one record per product ordered (many products per order), not to mention one sales rep placing many orders (many orders per sales rep), there will be duplicate records for our sales reps. Remove all duplicate records, making sure to exclude **`ingest_file_name`** and **`ingested_at`** from the evaluation of duplicate records
# MAGIC * Load the dataset to the managed delta table **`sales_rep_scd`** (identified by the variable **`sales_reps_table`**)
# MAGIC
# MAGIC **Additional Requirements:**<br/>
# MAGIC The schema for the **`sales_rep_scd`** table must be:
# MAGIC * **`sales_rep_id`**:**`string`**
# MAGIC * **`sales_rep_ssn`**:**`long`**
# MAGIC * **`sales_rep_first_name`**:**`string`**
# MAGIC * **`sales_rep_last_name`**:**`string`**
# MAGIC * **`sales_rep_address`**:**`string`**
# MAGIC * **`sales_rep_city`**:**`string`**
# MAGIC * **`sales_rep_state`**:**`string`**
# MAGIC * **`sales_rep_zip`**:**`integer`**
# MAGIC * **`ingest_file_name`**:**`string`**
# MAGIC * **`ingested_at`**:**`timestamp`**
# MAGIC * **`_error_ssn_format`**:**`boolean`**

# COMMAND ----------

t = spark.table(batch_temp_view)
t.count()

# COMMAND ----------

t = t.withColumn("_error_ssn_format", t["sales_rep_ssn"].contains("-"))
t = t.withColumn("_err_ssn_int", t["_error_ssn_format"].astype("integer"))
t.columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

t.groupBy().sum("_err_ssn_int").show()

# COMMAND ----------

import pyspark
import pandas as pd

# COMMAND ----------

@pyspark.sql.functions.pandas_udf('string')
def remove_hyphen(s: pd.Series) -> pd.Series:
    t = s.str.replace("-", "")
    return t

# COMMAND ----------

t = t.withColumn("sales_rep_ssn_long", remove_hyphen(t["sales_rep_ssn"]).astype("long"))

# COMMAND ----------

t = t.withColumn("sales_rep_zip_int", t["sales_rep_zip"].astype("integer"))

# COMMAND ----------

cols = [
    "sales_rep_id",
    "sales_rep_ssn_long",
    "sales_rep_first_name",
    "sales_rep_last_name",
    "sales_rep_address",
    "sales_rep_city",
    "sales_rep_state",
    "sales_rep_zip_int",
    "ingest_file_name",
    "ingested_at",
    "_error_ssn_format",
]
unique_cols = [
    "sales_rep_id",
    "sales_rep_ssn_long",
    "sales_rep_first_name",
    "sales_rep_last_name",
    "sales_rep_address",
    "sales_rep_city",
    "sales_rep_state",
    "sales_rep_zip_int",
]

# COMMAND ----------

t.count()

# COMMAND ----------

# deduplication
u = t[cols].dropDuplicates(subset=unique_cols)

# COMMAND ----------

u.count()

# COMMAND ----------

# rename retyped columns
u = u.withColumnRenamed("sales_rep_ssn_long", "sales_rep_ssn").withColumnRenamed("sales_rep_zip_int", "sales_rep_zip")
u.columns

# COMMAND ----------

spark.sql(f"drop table if exists {sales_reps_table}")

# COMMAND ----------

u.write.mode("overwrite").saveAsTable(sales_reps_table)

# COMMAND ----------

reality_check_03_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.D - Extract Orders</h2>
# MAGIC
# MAGIC Our batched orders from Exercise 02 contains one line per product meaning there are multiple records per order.
# MAGIC
# MAGIC The goal of this step is to extract just the order details (excluding the sales rep and line items)
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`submitted_at`** is a "unix epoch" (number of seconds since 1970-01-01 00:00:00 UTC) and should be represented as a **`Timestamp`**
# MAGIC   * The column **`shipping_address_zip`** should be represented as an **`Integer`**
# MAGIC * Remove the columns not directly related to the order record:
# MAGIC   * Sales reps columns: **`sales_rep_ssn`**, **`sales_rep_first_name`**, **`sales_rep_last_name`**, **`sales_rep_address`**, **`sales_rep_city`**, **`sales_rep_state`**, **`sales_rep_zip`**
# MAGIC   * Product columns: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * Because there is one record per product ordered (many products per order), there will be duplicate records for each order. Remove all duplicate records, making sure to exclude **`ingest_file_name`** and **`ingested_at`** from the evaluation of duplicate records
# MAGIC * Add the column **`submitted_yyyy_mm`** which is a **`string`** derived from **`submitted_at`** and is formatted as "**yyyy-MM**".
# MAGIC * Load the dataset to the managed delta table **`orders`** (identified by the variable **`orders_table`**)
# MAGIC   * In thise case, the data must also be partitioned by **`submitted_yyyy_mm`**
# MAGIC
# MAGIC **Additional Requirements:**
# MAGIC * The schema for the **`orders`** table must be:
# MAGIC   * **`submitted_at:timestamp`**
# MAGIC   * **`submitted_yyyy_mm`** using the format "**yyyy-MM**"
# MAGIC   * **`order_id:string`**
# MAGIC   * **`customer_id:string`**
# MAGIC   * **`sales_rep_id:string`**
# MAGIC   * **`shipping_address_attention:string`**
# MAGIC   * **`shipping_address_address:string`**
# MAGIC   * **`shipping_address_city:string`**
# MAGIC   * **`shipping_address_state:string`**
# MAGIC   * **`shipping_address_zip:integer`**
# MAGIC   * **`ingest_file_name:string`**
# MAGIC   * **`ingested_at:timestamp`**

# COMMAND ----------

t = spark.table(batch_temp_view)
t.count()

# COMMAND ----------

t.select("submitted_at").head(10)

# COMMAND ----------

x = pyspark.sql.functions.to_timestamp(t["submitted_at"].cast("integer"))
u = t.withColumn("submitted_at", x)
u.select("submitted_at").head(10)

# COMMAND ----------

t.select("shipping_address_zip").head(10)

# COMMAND ----------

x = t["shipping_address_zip"].astype("integer")
u = u.withColumn("shipping_address_zip", x)
u.select("shipping_address_zip").head(10)

# COMMAND ----------

order_cols_unique = [
    "submitted_at",
    # "submitted_yyyy_mm",
    "order_id",
    "customer_id",
    "sales_rep_id",
    "shipping_address_attention",
    "shipping_address_address",
    "shipping_address_city",
    "shipping_address_state",
    "shipping_address_zip",
]
order_cols = order_cols_unique + [
    "ingest_file_name",
    "ingested_at",
]

# COMMAND ----------

orders = u[order_cols].dropDuplicates(subset=order_cols_unique)
orders = orders.withColumn("submitted_yyyy_mm", pyspark.sql.functions.date_format(orders["submitted_at"], "yyyy-MM"))

# COMMAND ----------

orders.count()

# COMMAND ----------

orders.head(5)

# COMMAND ----------

spark.sql(f"drop table if exists {orders_table}")
orders.write.mode("overwrite").saveAsTable(orders_table, partitionBy="submitted_yyyy_mm")

# COMMAND ----------

reality_check_03_d()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.E - Extract Line Items</h2>
# MAGIC
# MAGIC Now that we have extracted sales reps and orders, we next want to extract the specific line items of each order.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * Retain the following columns (see schema below)
# MAGIC   * The correlating ID columns: **`order_id`** and **`product_id`**
# MAGIC   * The two product-specific columns: **`product_quantity`** and **`product_sold_price`**
# MAGIC   * The two ingest columns: **`ingest_file_name`** and **`ingested_at`**
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`product_quantity`** should be represented as an **`Integer`**
# MAGIC   * The column **`product_sold_price`** should be represented as an **`Decimal`** with two decimal places as in **`decimal(10,2)`**
# MAGIC * Load the dataset to the managed delta table **`line_items`** (identified by the variable **`line_items_table`**)
# MAGIC
# MAGIC **Additional Requirements:**
# MAGIC * The schema for the **`line_items`** table must be:
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`integer`**
# MAGIC   * **`product_sold_price`**:**`decimal(10,2)`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

line_item_columns = [
    "order_id",
    "product_id",
    "product_quantity",
    "product_sold_price",
    "ingest_file_name",
    "ingested_at",
]

# COMMAND ----------

t = spark.table(batch_temp_view)

# COMMAND ----------

u = t[line_item_columns].withColumn("product_quantity", t["product_quantity"].astype("integer"))
x = t["product_sold_price"].astype("decimal(10,2)")
u = u.withColumn("product_sold_price", x)

# COMMAND ----------

spark.sql(f"drop table if exists {line_items_table}")
u.write.mode("overwrite").saveAsTable(line_items_table)

# COMMAND ----------

reality_check_03_e()

# COMMAND ----------

reality_check_03_final()
