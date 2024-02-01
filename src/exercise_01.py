from pyspark.sql.functions import trim, col, when, sum, input_file_name, current_timestamp
import common


def trafo(x):
    y1 = trim(x)
    y2 = when(y1 != "", y1).otherwise(None)
    return y1


def create_2017():
    df1 = spark.read.text(batch_2017_path)
    df2 = df1
    for name, (p1, p2) in fixed_width_column_defs.items():
        df2 = df2.withColumn(name, trafo(df1.value.substr(p1, p2)))
    df2 = df2.withColumn("ingest_file_name", input_file_name())
    df2 = df2.withColumn("ingested_at", current_timestamp())
    return df2


def create_2018():
    dfb1 = spark.read.csv(batch_2018_path, sep="\t", header=True)
    dfb2 = dfb1
    for name in dfb1.schema.names:
        dfb2 = dfb2.withColumn(name, trafo(dfb1[name]))
    dfb2 = dfb2.withColumn("ingest_file_name", input_file_name())
    dfb2 = dfb2.withColumn("ingested_at", current_timestamp())
    return dfb2


def create_2019():
    dfc1 = spark.read.csv(batch_2019_path, sep=",", header=True)
    dfc2 = dfc1
    for na, nb in zip(dfc1.schema.names, dfb2.schema.names):
        dfc2 = dfc2.withColumnRenamed(na, nb)
        dfc2 = dfc2.withColumn(nb, trafo(dfc2[nb]))
    dfc2 = dfc2.withColumn("ingest_file_name", input_file_name())
    dfc2 = dfc2.withColumn("ingested_at", current_timestamp())
    return dfc2


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
    "product_sold_price": (390, 20),
}
df_2017 = create_2017()
df_2018 = create_2018()
df_2018 = create_2019()
df_2017.write.mode("overwrite").parquet(batch_target_path)
df_2018.write.mode("append").parquet(batch_target_path)
df_2019.write.mode("append").parquet(batch_target_path)