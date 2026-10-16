# Databricks notebook source
"""Publish exactly one new input batch into the Auto Loader landing zone."""

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "delivery360")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("governance_schema", "governance")

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
governance = dbutils.widgets.get("governance_schema")
volume = f"/Volumes/{catalog}/{bronze}/raw_files"
control = f"`{catalog}`.`{governance}`.`batch_control`"

state = spark.table(control).where("pipeline_name = 'delivery360'").limit(1).collect()
last_batch = int(state[0]["last_batch_id"]) if state else 0
next_batch = last_batch + 1
if next_batch > 3:
    raise ValueError("All seeded batches have already been published. Run the generator again to reset the demo.")

batch = f"batch_{next_batch:03d}"
source = f"{volume}/seed/batches/{batch}"
destination = f"{volume}/incoming/{batch}"

if next_batch == 1:
    dbutils.fs.cp(f"{volume}/seed/reference", f"{volume}/incoming/reference", recurse=True)
dbutils.fs.cp(source, destination, recurse=True)

spark.sql(f"DELETE FROM {control} WHERE pipeline_name = 'delivery360'")
spark.createDataFrame([("delivery360", next_batch)], ["pipeline_name", "last_batch_id"]).withColumn(
    "updated_at", F.current_timestamp()
).write.mode("append").saveAsTable(control)

print(f"Published {batch} to {destination}")
