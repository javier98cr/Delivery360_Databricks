# Databricks notebook source
"""Create reproducible seed data for Delivery360.

Run once before the first pipeline execution. The simulator publishes these
seed files into the incoming folder one batch at a time.
"""

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "delivery360")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("governance_schema", "governance")

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
governance = dbutils.widgets.get("governance_schema")
volume = f"/Volumes/{catalog}/{bronze}/raw_files"
seed = f"{volume}/seed"

for schema in [bronze, "silver", "gold", governance]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{bronze}`.`raw_files`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{bronze}`.`checkpoints`")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{governance}`.`batch_control` (
  pipeline_name STRING,
  last_batch_id INT,
  updated_at TIMESTAMP
) USING DELTA
""")

names = F.array(*[F.lit(x) for x in ["Ana", "Bruno", "Carla", "Diego", "Elena", "Felipe"]])
cities = F.array(*[F.lit(x) for x in ["San Jose", "Heredia", "Alajuela", "Cartago"]])

customers = (
    spark.range(1, 2001)
    .select(
        F.concat(F.lit("CUST-"), F.lpad("id", 5, "0")).alias("customer_id"),
        F.element_at(names, (F.col("id") % 6 + 1).cast("int")).alias("first_name"),
        F.concat(F.lit("customer"), F.col("id"), F.lit("@delivery360.example")).alias("email"),
        F.concat(F.lit("+506-88"), F.lpad((F.col("id") % 100000).cast("string"), 5, "0")).alias("phone"),
        F.element_at(cities, (F.col("id") % 4 + 1).cast("int")).alias("region"),
    )
)

restaurants = (
    spark.range(1, 101)
    .select(
        F.concat(F.lit("REST-"), F.lpad("id", 4, "0")).alias("restaurant_id"),
        F.concat(F.lit("Restaurant "), F.col("id")).alias("restaurant_name"),
        F.element_at(cities, (F.col("id") % 4 + 1).cast("int")).alias("region"),
        F.element_at(F.array(*[F.lit(x) for x in ["pizza", "healthy", "burgers", "local"]]), (F.col("id") % 4 + 1).cast("int")).alias("category"),
    )
)

drivers = (
    spark.range(1, 301)
    .select(
        F.concat(F.lit("DRV-"), F.lpad("id", 4, "0")).alias("driver_id"),
        F.concat(F.lit("driver"), F.col("id"), F.lit("@delivery360.example")).alias("email"),
        F.element_at(cities, (F.col("id") % 4 + 1).cast("int")).alias("region"),
        (F.lit(4.50) + (F.col("id") % 10) / 10).cast("decimal(8,2)").alias("base_fee"),
    )
)

for df, path in [
    (customers, f"{seed}/reference/customers"),
    (restaurants, f"{seed}/reference/restaurants"),
    (drivers, f"{seed}/reference/drivers"),
]:
    df.write.mode("overwrite").json(path)

for batch_id in range(1, 4):
    orders = (
        spark.range(1, 501)
        .select(
            F.concat(F.lit(f"ORD-{batch_id:03d}-"), F.lpad("id", 5, "0")).alias("order_id"),
            F.when(F.col("id") % 53 == 0, F.lit(None).cast("string"))
             .otherwise(F.concat(F.lit("CUST-"), F.lpad(((F.col("id") * 7) % 2000 + 1).cast("string"), 5, "0"))).alias("customer_id"),
            F.concat(F.lit("REST-"), F.lpad(((F.col("id") * 11) % 100 + 1).cast("string"), 4, "0")).alias("restaurant_id"),
            F.concat(F.lit("DRV-"), F.lpad(((F.col("id") * 13) % 300 + 1).cast("string"), 4, "0")).alias("driver_id"),
            F.to_timestamp(F.lit(f"2026-01-{batch_id + 1:02d} 10:00:00")).alias("order_ts"),
            F.when(F.col("id") % 47 == 0, F.lit(-12.00))
             .otherwise((F.lit(8.00) + (F.col("id") * 37 % 18000) / 100)).cast("decimal(12,2)").alias("order_total"),
            F.when(F.col("id") % 41 == 0, F.lit("UNKNOWN"))
             .when(F.col("id") % 17 == 0, F.lit("CANCELLED"))
             .otherwise(F.lit("DELIVERED")).alias("order_status"),
            ((F.col("id") * 5) % 70 + 20).cast("int").alias("delivery_minutes"),
        )
    )
    payments = orders.select(
        "order_id",
        F.when(F.col("order_id").endswith("7"), F.lit("cash")).otherwise(F.lit("card")).alias("payment_method"),
        "order_total",
        F.when(F.col("order_status") == "CANCELLED", F.lit("refunded")).otherwise(F.lit("captured")).alias("payment_status"),
    )
    events = orders.select(
        "order_id", "order_ts",
        F.lit("created").alias("event_type"),
        F.current_timestamp().alias("event_ts"),
    )
    orders.write.mode("overwrite").json(f"{seed}/batches/batch_{batch_id:03d}/orders")
    payments.write.mode("overwrite").json(f"{seed}/batches/batch_{batch_id:03d}/payments")
    events.write.mode("overwrite").json(f"{seed}/batches/batch_{batch_id:03d}/order_events")

spark.sql(f"""
MERGE INTO `{catalog}`.`{governance}`.`batch_control` AS target
USING (SELECT 'delivery360' AS pipeline_name, 0 AS last_batch_id, current_timestamp() AS updated_at) AS source
ON target.pipeline_name = source.pipeline_name
WHEN MATCHED THEN UPDATE SET last_batch_id = 0, updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

print(f"Seed data created under {seed}")
