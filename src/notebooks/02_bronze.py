# Databricks notebook source
"""Incrementally ingest landing files with Auto Loader and availableNow."""

from pyspark.sql import types as T

dbutils.widgets.text("catalog", "delivery360")
dbutils.widgets.text("bronze_schema", "bronze")
catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
base = f"/Volumes/{catalog}/{bronze}/raw_files/incoming"
checkpoint_base = f"/Volumes/{catalog}/{bronze}/checkpoints/bronze"

schemas = {
    "orders": T.StructType([
        T.StructField("order_id", T.StringType()), T.StructField("customer_id", T.StringType()),
        T.StructField("restaurant_id", T.StringType()), T.StructField("driver_id", T.StringType()),
        T.StructField("order_ts", T.TimestampType()), T.StructField("order_total", T.DecimalType(12, 2)),
        T.StructField("order_status", T.StringType()), T.StructField("delivery_minutes", T.IntegerType()),
    ]),
    "payments": T.StructType([
        T.StructField("order_id", T.StringType()), T.StructField("payment_method", T.StringType()),
        T.StructField("order_total", T.DecimalType(12, 2)), T.StructField("payment_status", T.StringType()),
    ]),
    "order_events": T.StructType([
        T.StructField("order_id", T.StringType()), T.StructField("order_ts", T.TimestampType()),
        T.StructField("event_type", T.StringType()), T.StructField("event_ts", T.TimestampType()),
    ]),
    "customers": T.StructType([
        T.StructField("customer_id", T.StringType()), T.StructField("first_name", T.StringType()),
        T.StructField("email", T.StringType()), T.StructField("phone", T.StringType()), T.StructField("region", T.StringType()),
    ]),
    "restaurants": T.StructType([
        T.StructField("restaurant_id", T.StringType()), T.StructField("restaurant_name", T.StringType()),
        T.StructField("region", T.StringType()), T.StructField("category", T.StringType()),
    ]),
    "drivers": T.StructType([
        T.StructField("driver_id", T.StringType()), T.StructField("email", T.StringType()),
        T.StructField("region", T.StringType()), T.StructField("base_fee", T.DecimalType(8, 2)),
    ]),
}

for name, schema in schemas.items():
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/{name}/schema")
        .schema(schema)
        .load(f"{base}/*/{name}")
        .selectExpr("*", "_metadata.file_path AS source_file", "_metadata.file_name AS source_file_name")
        .writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/{name}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"`{catalog}`.`{bronze}`.`{name}`")
    )
    query.awaitTermination()

print("Bronze ingestion completed with Auto Loader and availableNow")
