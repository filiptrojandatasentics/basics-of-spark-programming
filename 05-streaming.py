# Databricks notebook source
# MAGIC %md 
# MAGIC # Exercise #5 - Streaming Orders
# MAGIC
# MAGIC With our four historical datasets properly loaded, we can now begin to process the "current" orders.
# MAGIC
# MAGIC In this case, the new "system" is landing one JSON file per order into cloud storage.
# MAGIC
# MAGIC We can process these JSON files as a stream of orders under the assumption that new orders are continually added to this dataset.
# MAGIC
# MAGIC In order to keep this project simple, we have reduced the "stream" of orders to just the first few hours of 2020 and will be throttling that stream to only one file per iteration.
# MAGIC
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 5.A - Use Database
# MAGIC * Exercise 5.B - Stream-Append Orders
# MAGIC * Exercise 5.C - Stream-Append Line Items
# MAGIC
# MAGIC ## Some Friendly Advice...
# MAGIC
# MAGIC Each record is a JSON object with roughly the following structure:
# MAGIC
# MAGIC * **`customerID`**
# MAGIC * **`orderId`**
# MAGIC * **`products`**
# MAGIC   * array
# MAGIC     * **`productId`**
# MAGIC     * **`quantity`**
# MAGIC     * **`soldPrice`**
# MAGIC * **`salesRepId`**
# MAGIC * **`shippingAddress`**
# MAGIC   * **`address`**
# MAGIC   * **`attention`**
# MAGIC   * **`city`**
# MAGIC   * **`state`**
# MAGIC   * **`zip`**
# MAGIC * **`submittedAt`**
# MAGIC
# MAGIC As you ingest this data, it will need to be transformed to match the existing **`orders`** table's schema and the **`line_items`** table's schema.
# MAGIC
# MAGIC Before attempting to ingest the data as a stream, we highly recomend that you start with a static **`DataFrame`** so that you can iron out the various kinks:
# MAGIC * Renaming and flattening columns
# MAGIC * Exploding the products array
# MAGIC * Parsing the **`submittedAt`** column into a **`timestamp`**
# MAGIC * Conforming to the **`orders`** and **`line_items`** schemas - because these are Delta tables, appending to them will fail if the schemas are not correct
# MAGIC
# MAGIC Furthermore, creating a stream from JSON files will first require you to specify the schema - you can "cheat" and infer that schema from some of the JSON files before starting the stream.

# COMMAND ----------

!pwd

# COMMAND ----------

# MAGIC %run ./Setup-Exercise-05

# COMMAND ----------

spark.sql(f"use {user_db}")

# COMMAND ----------

h = spark.sql(f"describe history {orders_table}")
display(h)

# COMMAND ----------

display(spark.sql(f"select count(*) FROM {orders_table} VERSION AS OF 0"))

# COMMAND ----------

spark.sql(f"RESTORE TABLE {orders_table} TO VERSION AS OF 0")

# COMMAND ----------

display(spark.sql(f"select count(*) FROM {orders_table}"))

# COMMAND ----------

h = spark.sql(f"describe history {orders_table}")
display(h)

# COMMAND ----------

display(spark.sql(f"describe history {line_items_table}"))

# COMMAND ----------

spark.sql(f"restore table {line_items_table} to version as of 0")

# COMMAND ----------

reality_check_05_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5.B - Stream-Append Orders</h2>
# MAGIC
# MAGIC Every JSON file ingested by our stream representes one order and the enumerated list of products purchased in that order.
# MAGIC
# MAGIC Our goal is simple, ingest the data, transform it as required by the **`orders`** table's schema, and append these new records to our existing table.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC
# MAGIC * Ingest the stream of JSON files:
# MAGIC   * Start a stream from the path identified by **`stream_path`**.
# MAGIC   * Using the **`maxFilesPerTrigger`** option, throttle the stream to process only one file per iteration.
# MAGIC   * Add the ingest meta data (same as with our other datasets):
# MAGIC     * **`ingested_at`**:**`timestamp`**
# MAGIC     * **`ingest_file_name`**:**`string`**
# MAGIC   * Properly parse the **`submitted_at`**  as a valid **`timestamp`**
# MAGIC   * Add the column **`submitted_yyyy_mm`** usinge the format "**yyyy-MM**"
# MAGIC   * Make any other changes required to the column names and data types so that they conform to the **`orders`** table's schema
# MAGIC
# MAGIC * Write the stream to a Delta **table**.:
# MAGIC   * The table's format should be "**delta**"
# MAGIC   * Partition the data by the column **`submitted_yyyy_mm`**
# MAGIC   * Records must be appended to the table identified by the variable **`orders_table`**
# MAGIC   * The query must be named the same as the table, identified by the variable **`orders_table`**
# MAGIC   * The query must use the checkpoint location identified by the variable **`orders_checkpoint_path`**

# COMMAND ----------

orders = spark.sql(f"select * from {orders_table}")

# COMMAND ----------

orders.count()

# COMMAND ----------

stream_path

# COMMAND ----------

files = dbutils.fs.ls(stream_path)
files

# COMMAND ----------

dbutils.fs.head(files[0].path)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, ArrayType

# COMMAND ----------

products_schema = StructType([
    StructField(name="productId", dataType=StringType(), nullable=True),
    StructField(name="quantity", dataType=IntegerType(), nullable=True),
    StructField(name="soldPrice", dataType=FloatType(), nullable=True),
])
address_schema = StructType([
    StructField(name="attention", dataType=StringType(), nullable=True),
    StructField(name="address", dataType=StringType(), nullable=True),
    StructField(name="city", dataType=StringType(), nullable=True),
    StructField(name="state", dataType=StringType(), nullable=True),
    StructField(name="zip", dataType=StringType(), nullable=True),
])
orders_json_schema = StructType([
    StructField(name="customerId", dataType=StringType(), nullable=False),
    StructField(name="orderId", dataType=StringType(), nullable=False),
    StructField(name="salesRepId", dataType=StringType(), nullable=False),
    StructField(name="submittedAt", dataType=TimestampType(), nullable=False),
    StructField(name="products", dataType=ArrayType(products_schema), nullable=True),
    StructField(name="shippingAddress", dataType=address_schema, nullable=True),
])

# COMMAND ----------

df = spark.readStream.option("maxFilesPerTrigger", 1).format("delta").json(stream_path, schema=orders_json_schema)

# COMMAND ----------

df.isStreaming

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp

# COMMAND ----------

out_df = df.withColumnRenamed("submittedAt", "submitted_at")
out_df = out_df.withColumnRenamed("orderId", "order_id")
out_df = out_df.withColumnRenamed("customerId", "customer_id")
out_df = out_df.withColumnRenamed("salesRepId", "sales_rep_id")
out_df = out_df.withColumn("shipping_address_attention", out_df.shippingAddress.attention)
out_df = out_df.withColumn("shipping_address_address", out_df.shippingAddress.address)
out_df = out_df.withColumn("shipping_address_city", out_df.shippingAddress.city)
out_df = out_df.withColumn("shipping_address_state", out_df.shippingAddress.state)
out_df = out_df.withColumn("shipping_address_zip", out_df.shippingAddress.zip.astype("integer"))
out_df = out_df.withColumn("ingest_file_name", input_file_name())
out_df = out_df.withColumn("ingested_at", current_timestamp())
out_df = out_df.withColumn("submitted_yyyy_mm", pyspark.sql.functions.date_format(out_df["submitted_at"], "yyyy-MM"))

# COMMAND ----------

out_df.isStreaming

# COMMAND ----------

orders_out_df = out_df[[
    "submitted_at",
    "order_id",
    "customer_id",
    "sales_rep_id",
    "shipping_address_attention",
    "shipping_address_address",
    "shipping_address_city",
    "shipping_address_state",
    "shipping_address_zip",
    "ingest_file_name",
    "ingested_at",
    "submitted_yyyy_mm",
]]

# COMMAND ----------

orders.schema == orders_out_df.schema

# COMMAND ----------

orders_out_df.schema['ingest_file_name'].nullable = True
orders_out_df.schema['ingested_at'].nullable = True

# COMMAND ----------

orders.schema == orders_out_df.schema

# COMMAND ----------

orders_query = orders_out_df.writeStream. \
    outputMode("append").format("delta").partitionBy("submitted_yyyy_mm").queryName(orders_table).trigger(processingTime="1 second"). \
    option("checkpointLocation", orders_checkpoint_path).toTable(tableName=orders_table)

# COMMAND ----------

orders_query.id

# COMMAND ----------

orders_query.status

# COMMAND ----------

orders_query.lastProgress

# COMMAND ----------

reality_check_05_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5.C - Stream-Append Line Items</h2>
# MAGIC
# MAGIC The same JSON file we processed in the previous stream also contains the line items which we now need to extract and append to the existing **`line_items`** table.
# MAGIC
# MAGIC Just like before, our goal is simple, ingest the data, transform it as required by the **`line_items`** table's schema, and append these new records to our existing table.
# MAGIC
# MAGIC Note: we are processing the same stream twice - there are other patterns to do this more efficiently, but for this exercise, we want to keep the design simple.<br/>
# MAGIC The good news here is that you can copy most of the code from the previous step to get you started here.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC
# MAGIC * Ingest the stream of JSON files:
# MAGIC   * Start a stream from the path identified by **`stream_path`**.
# MAGIC   * Using the **`maxFilesPerTrigger`** option, throttle the stream to process only one file per iteration.
# MAGIC   * Add the ingest meta data (same as with our other datasets):
# MAGIC     * **`ingested_at`**:**`timestamp`**
# MAGIC     * **`ingest_file_name`**:**`string`**
# MAGIC   * Make any other changes required to the column names and data types so that they conform to the **`line_items`** table's schema
# MAGIC     * The most significant transformation will be to the **`products`** column.
# MAGIC     * The **`products`** column is an array of elements and needs to be exploded (see **`pyspark.sql.functions`**)
# MAGIC     * One solution would include:
# MAGIC       1. Select **`order_id`** and explode **`products`** while renaming it to **`product`**.
# MAGIC       2. Flatten the **`product`** column's nested values.
# MAGIC       3. Add the ingest meta data (**`ingest_file_name`** and **`ingested_at`**).
# MAGIC       4. Convert data types as required by the **`line_items`** table's schema.
# MAGIC
# MAGIC * Write the stream to a Delta sink:
# MAGIC   * The sink's format should be "**delta**"
# MAGIC   * Records must be appended to the table identified by the variable **`line_items_table`**
# MAGIC   * The query must be named the same as the table, identified by the variable **`line_items_table`**
# MAGIC   * The query must use the checkpoint location identified by the variable **`line_items_checkpoint_path`**

# COMMAND ----------

line_items = spark.sql(f"select * from {line_items_table}")
line_items.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

out_df_line_items = out_df.select(out_df.order_id, out_df.ingest_file_name, out_df.ingested_at, explode(out_df.products))
out_df_line_items = out_df_line_items.withColumn("product_id", out_df_line_items.col.productId)
out_df_line_items = out_df_line_items.withColumn("product_quantity", out_df_line_items.col.quantity)
out_df_line_items = out_df_line_items.withColumn("product_sold_price", out_df_line_items.col.soldPrice.astype("decimal(10,2)"))

# COMMAND ----------

line_items_out_df = out_df_line_items[[
    "order_id",
    "product_id",
    "product_quantity",
    "product_sold_price",
    "ingest_file_name",
    "ingested_at",
]]
line_items_out_df.printSchema()

# COMMAND ----------

line_items_query = line_items_out_df.writeStream. \
    outputMode("append").format("delta").queryName(line_items_table).trigger(processingTime="1 second"). \
    option("checkpointLocation", line_items_checkpoint_path).toTable(tableName=line_items_table)

# COMMAND ----------

reality_check_05_c()

# COMMAND ----------

line_items.count()

# COMMAND ----------

reality_check_05_final()

# COMMAND ----------


