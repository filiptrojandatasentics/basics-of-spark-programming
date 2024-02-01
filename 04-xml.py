# Databricks notebook source
# MAGIC %md 
# MAGIC # Exercise #4 - XML Ingestion, Products Table
# MAGIC
# MAGIC The products being sold by our sales reps are itemized in an XML document which we will need to load.
# MAGIC
# MAGIC Unlike CSV, JSON, Parquet, & Delta, support for XML is not included with the default distribution of Apache Spark.
# MAGIC
# MAGIC Before we can load the XML document, we need additional support for a **`DataFrameReader`** that can processes XML files.
# MAGIC
# MAGIC Once the **spark-xml** library is installed to our cluster, we can load our XML document and proceede with our other transformations.
# MAGIC
# MAGIC This exercise is broken up into 4 steps:
# MAGIC * Exercise 4.A - Use Database
# MAGIC * Exercise 4.B - Install Library
# MAGIC * Exercise 4.C - Load Products
# MAGIC * Exercise 4.D - Load ProductLineItems

# COMMAND ----------

# MAGIC %run ./Setup-Exercise-04

# COMMAND ----------

spark.sql(f"use {user_db}")

# COMMAND ----------

reality_check_04_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.B - Install Library</h2>
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Register the **spark-xml** library - edit your cluster configuration and then from the **Libraries** tab, install the following library:
# MAGIC   * Type: **Maven**
# MAGIC   * Coordinates: **com.databricks:spark-xml_2.12:0.10.0**
# MAGIC
# MAGIC If you are unfamiliar with this processes, more information can be found in the <a href="https://docs.databricks.com/libraries/cluster-libraries.html" target="_blank">Cluster libraries documentation</a>.
# MAGIC
# MAGIC Once the library is installed, run the following reality check to confirm proper installation.<br/>
# MAGIC Note: You may need to restart the cluster after installing the library for you changes to take effect.
# MAGIC
# MAGIC Filip: Checked Compute/Capstone/Libraruies and it is indeed already installed as expected.

# COMMAND ----------

reality_check_04_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.C - Load Products</h2>
# MAGIC
# MAGIC With the **spark-xml** library installed, ingesting an XML document is identical to ingesting any other dataset - other than specific, provided, options.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the XML document using the following paramters:
# MAGIC   * Format: **xml**
# MAGIC   * Options:
# MAGIC     * **`rootTag`** = **`products`** - identifies the root tag in the XML document, in our case this is "products"
# MAGIC     * **`rowTag`** = **`product`** - identifies the tag of each row under the root tag, in our case this is "product"
# MAGIC     * **`inferSchema`** = **`True`** - The file is small, and a one-shot operation - infering the schema will save us some time
# MAGIC   * File Path: specified by the variable **`products_xml_path`**
# MAGIC   
# MAGIC * Update the schema to conform to the following specification:
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`color`**:**`string`**
# MAGIC   * **`model_name`**:**`string`**
# MAGIC   * **`model_number`**:**`string`**
# MAGIC   * **`base_price`**:**`double`**
# MAGIC   * **`color_adj`**:**`double`**
# MAGIC   * **`size_adj`**:**`double`**
# MAGIC   * **`price`**:**`double`**
# MAGIC   * **`size`**:**`string`**
# MAGIC
# MAGIC * Exclude any records for which a **`price`** was not included - these represent products that are not yet available for sale.
# MAGIC * Load the dataset to the managed delta table **`products`** (identified by the variable **`products_table`**)

# COMMAND ----------

dbutils.fs.head(products_xml_path)

# COMMAND ----------

df = spark.read \
    .format('com.databricks.spark.xml') \
    .options(rootTag="products", rowTag="product", inferSchema=True) \
    .load(products_xml_path)
df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

df.price._base_price

# COMMAND ----------

p = df.withColumnRenamed("_product_id", "product_id")
p = p.withColumnRenamed("price", "p")
p = p.withColumn("base_price", p.p._base_price)
p = p.withColumn("color_adj", p.p._color_adj)
p = p.withColumn("size_adj", p.p._size_adj)
p = p.withColumn("price", p.p.usd)
p = p[["product_id", "color", "model_name", "model_number", "base_price", "color_adj", "size_adj", "price", "size"]]
p = p.filter(p.price.isNotNull())

# COMMAND ----------

p.count()

# COMMAND ----------

p.write.mode("overwrite").parquet(products_table)

# COMMAND ----------

spark.sql(f"drop table if exists {products_table}")

# COMMAND ----------

p.write.mode("overwrite").saveAsTable(products_table)

# COMMAND ----------

reality_check_04_c()

# COMMAND ----------

reality_check_04_final()
