# Databricks notebook source
# MAGIC %run ./Setup-Exercise-02

# COMMAND ----------

files = dbutils.fs.ls(f"{working_dir}/raw/orders/batch") # List all the files
display(files)                                           # Display the list of files

# COMMAND ----------

dbutils.fs.head(batch_2017_path)

# COMMAND ----------

fixed_width_column_defs = {
  "submitted_at": (1, 15),
  "order_id": (16, 40),
  "customer_id": (56, 40),
  "sales_rep_id": (96, 40),
  "sales_rep_ssn": (136, 15),
  "sales_rep_first_name": (151, 15),
  "sales_rep_last_name": (166, 15),
  "sales_rep_address": (181, 40),
  "sales_rep_city": (221, 20),
  "sales_rep_state": (241, 2),
  "sales_rep_zip": (243, 5),
  "shipping_address_attention": (248, 30),
  "shipping_address_address": (278, 40),
  "shipping_address_city": (318, 20),
  "shipping_address_state": (338, 2),
  "shipping_address_zip": (340, 5),
  "product_id": (345, 40),
  "product_quantity": (385, 5),
  "product_sold_price": (390, 20)
}

# COMMAND ----------

type(spark)

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

from pyspark.sql.functions import trim, col, when, sum, input_file_name, current_timestamp

# COMMAND ----------

def trafo(x):
    y1 = trim(x)
    y2 = when(y1 != "", y1).otherwise(None)
    return y1

# COMMAND ----------

df1 = spark.read.text(batch_2017_path)
df2 = df1
for name, (p1, p2) in fixed_width_column_defs.items():
    df2 = df2.withColumn(name, trafo(df1.value.substr(p1, p2)))
# df1.show()
df2 = df2.withColumn("ingest_file_name", input_file_name())
df2 = df2.withColumn("ingested_at", current_timestamp())
df2.show()

# COMMAND ----------

a = df2.select([sum(col(c).isNull().cast("integer")).alias(c) for c in df2.columns]).show()
display(a)

# COMMAND ----------

df2.write.mode("overwrite").parquet(batch_target_path)

# COMMAND ----------

dbutils.fs.ls(batch_target_path)

# COMMAND ----------

df2.count()

# COMMAND ----------

reality_check_02_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.B - Ingest Tab-Separted File</h2>
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2018_path`**, and **`dbutils.fs.head`** to investigate the 2018 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the tab-separated file identified by **`batch_2018_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created datasets specified by **`batch_target_path`**
# MAGIC
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**

# COMMAND ----------

dbutils.fs.head(batch_2018_path)

# COMMAND ----------

dfb1 = spark.read.csv(batch_2018_path, sep="\t", header=True)
dfb1.show()

# COMMAND ----------

dfb1.count()

# COMMAND ----------

dfb2 = dfb1
for name in dfb1.schema.names:
    dfb2 = dfb2.withColumn(name, trafo(dfb1[name]))
dfb2 = dfb2.withColumn("ingest_file_name", input_file_name())
dfb2 = dfb2.withColumn("ingested_at", current_timestamp())
dfb2.show()

# COMMAND ----------

dfb2.write.mode("append").parquet(batch_target_path)

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.C - Ingest Comma-Separted File</h2>
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2019_path`**, and **`dbutils.fs.head`** to investigate the 2019 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the comma-separated file identified by **`batch_2019_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created dataset specified by **`batch_target_path`**<br/>
# MAGIC    Note: The column names in this dataset must be updated to conform to the schema defined for Exercise #2.A - there are several strategies for this:
# MAGIC    * Provide a schema that alters the names upon ingestion
# MAGIC    * Manually rename one column at a time
# MAGIC    * Use **`fixed_width_column_defs`** programaticly rename one column at a time
# MAGIC    * Use transformations found in the **`DataFrame`** class to rename all columns in one operation
# MAGIC
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**<br/>

# COMMAND ----------

dbutils.fs.head(batch_2019_path)

# COMMAND ----------

dfc1 = spark.read.csv(batch_2019_path, sep=",", header=True)
dfc1.show()

# COMMAND ----------

for na, nb in zip(dfb2.schema.names, dfc1.schema.names):
    if na != nb:
        print(f"{na} <- {nb}")
    else:
        print(na)

# COMMAND ----------

dfc2 = dfc1
for na, nb in zip(dfc1.schema.names, dfb2.schema.names):
    dfc2 = dfc2.withColumnRenamed(na, nb)
    dfc2 = dfc2.withColumn(nb, trafo(dfc2[nb]))
dfc2 = dfc2.withColumn("ingest_file_name", input_file_name())
dfc2 = dfc2.withColumn("ingested_at", current_timestamp())
dfc2.show()

# COMMAND ----------

dfc2.count()

# COMMAND ----------

dfc2.write.mode("append").parquet(batch_target_path)

# COMMAND ----------

reality_check_02_final()

# COMMAND ----------

df = spark.read.parquet(batch_target_path)
df.count()

# COMMAND ----------

df.columns

# COMMAND ----------


