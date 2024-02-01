# Databricks notebook source
# MAGIC %md 
# MAGIC # Exercise #6 - Business Questions
# MAGIC
# MAGIC In our last exercise, we are going to execute various joins across our four tables (**`orders`**, **`line_items`**, **`sales_reps`** and **`products`**) to answer basic business questions
# MAGIC
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 6.A - Use Database
# MAGIC * Exercise 6.B - Question #1
# MAGIC * Exercise 6.C - Question #2
# MAGIC * Exercise 6.D - Question #3

# COMMAND ----------

# MAGIC %run ./Setup-Exercise-06

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.A - Use Database</h2>
# MAGIC
# MAGIC Each notebook uses a different Spark session and will initially use the **`default`** database.
# MAGIC
# MAGIC As in the previous exercise, we can avoid contention to commonly named tables by using our user-specific database.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database

# COMMAND ----------

spark.sql(f"use {user_db}")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

reality_check_06_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.B - Question #1</h2>
# MAGIC ## How many orders were shipped to each state?
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Aggregate the orders by **`shipping_address_state`**
# MAGIC * Count the number of records for each state
# MAGIC * Sort the results by **`count`**, in descending order
# MAGIC * Save the results to the temporary view **`question_1_results`**, identified by the variable **`question_1_results_table`**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view question_1_results as
# MAGIC select shipping_address_state, count(1) as n_orders
# MAGIC from orders
# MAGIC group by shipping_address_state
# MAGIC order by count(1) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from question_1_results

# COMMAND ----------

reality_check_06_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.C - Question #2</h2>
# MAGIC ## What is the average, minimum and maximum sale price for green products sold to North Carolina where the Sales Rep submitted an invalid Social Security Number (SSN)?
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Execute a join across all four tables:
# MAGIC   * **`orders`**, identified by the variable **`orders_table`**
# MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
# MAGIC   * **`products`**, identified by the variable **`products_table`**
# MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
# MAGIC * Limit the result to only green products (**`color`**).
# MAGIC * Limit the result to orders shipped to North Carolina (**`shipping_address_state`**)
# MAGIC * Limit the result to sales reps that initially submitted an improperly formatted SSN (**`_error_ssn_format`**)
# MAGIC * Calculate the average, minimum and maximum of **`product_sold_price`** - do not rename these columns after computing.
# MAGIC * Save the results to the temporary view **`question_2_results`**, identified by the variable **`question_2_results_table`**
# MAGIC * The temporary view should have the following three columns: **`avg(product_sold_price)`**, **`min(product_sold_price)`**, **`max(product_sold_price)`**
# MAGIC * Collect the results to the driver
# MAGIC * Assign to the following local variables, the average, minimum, and maximum values - these variables will be passed to the reality check for validation.
# MAGIC  * **`ex_avg`** - the local variable holding the average value
# MAGIC  * **`ex_min`** - the local variable holding the minimum value
# MAGIC  * **`ex_max`** - the local variable holding the maximum value

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view question_2_results as
# MAGIC select avg(li.product_sold_price), min(li.product_sold_price), max(li.product_sold_price)
# MAGIC from line_items li
# MAGIC inner join products p on li.product_id = p.product_id
# MAGIC inner join orders o on li.order_id = o.order_id
# MAGIC inner join sales_reps r on o.sales_rep_id = r.sales_rep_id
# MAGIC where p.color = 'green' 
# MAGIC and o.shipping_address_state = 'NC'
# MAGIC -- and r._error_ssn_format

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from question_2_results

# COMMAND ----------

import pandas as pd

# COMMAND ----------

q2 = spark.sql("select * from question_2_results")
df2 = q2.collect()
for row in df2:
    ex_avg = row["avg(product_sold_price)"]
    ex_min = row["min(product_sold_price)"]
    ex_max = row["max(product_sold_price)"]
(ex_avg, ex_min, ex_max)

# COMMAND ----------

reality_check_06_c(ex_avg, ex_min, ex_max)

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.D - Question #3</h2>
# MAGIC ## What is the first and last name of the top earning sales rep based on net sales?
# MAGIC
# MAGIC For this scenario...
# MAGIC * The top earning sales rep will be identified as the individual producing the largest profit.
# MAGIC * Profit is defined as the difference between **`product_sold_price`** and **`price`** which is then<br/>
# MAGIC   multiplied by **`product_quantity`** as seen in **(product_sold_price - price) * product_quantity**
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Execute a join across all four tables:
# MAGIC   * **`orders`**, identified by the variable **`orders_table`**
# MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
# MAGIC   * **`products`**, identified by the variable **`products_table`**
# MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
# MAGIC * Calculate the profit for each line item of an order as described above.
# MAGIC * Aggregate the results by the sales reps' first &amp; last name and then sum each reps' total profit.
# MAGIC * Reduce the dataset to a single row for the sales rep with the largest profit.
# MAGIC * Save the results to the temporary view **`question_3_results`**, identified by the variable **`question_3_results_table`**
# MAGIC * The temporary view should have the following three columns: 
# MAGIC   * **`sales_rep_first_name`** - the first column by which to aggregate by
# MAGIC   * **`sales_rep_last_name`** - the second column by which to aggregate by
# MAGIC   * **`sum(total_profit)`** - the summation of the column **`total_profit`**

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table line_items

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view profit as
# MAGIC select r.sales_rep_first_name, r.sales_rep_last_name, sum((li.product_sold_price - p.price) * li.product_quantity) as profit
# MAGIC from line_items li
# MAGIC inner join products p on li.product_id = p.product_id
# MAGIC inner join orders o on li.order_id = o.order_id
# MAGIC inner join sales_reps r on o.sales_rep_id = r.sales_rep_id
# MAGIC group by r.sales_rep_first_name, r.sales_rep_last_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from profit order by profit desc

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view question_3_results as
# MAGIC select * from profit order by profit desc limit 1

# COMMAND ----------

reality_check_06_d()

# COMMAND ----------

reality_check_06_final()
