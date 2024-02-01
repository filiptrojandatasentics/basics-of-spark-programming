# Databricks notebook source
!git --version

# COMMAND ----------

!ls -l

# COMMAND ----------

!pwd

# COMMAND ----------

# MAGIC %md
# MAGIC Import files downloaded manually from https://github.com/DataSentics/Dbx-Academy_Developer-Essentials-Foundations-Capstones/tree/main/Developer-Foundations-Capstone/_includes

# COMMAND ----------

# MAGIC %run ./Setup-Exercise-01

# COMMAND ----------

install_datasets(reinstall=False)

# COMMAND ----------

!ls -l
