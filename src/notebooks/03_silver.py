# Databricks notebook source
"""Clean, validate, deduplicate and enrich Bronze data."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "delivery360")
catalog = dbutils.widgets.get("catalog")
bronze = "bronze"
silver = "silver"

orders_raw = spark.table(f"`{catalog}`.`{bronze}`.`orders`")
before = orders_raw.count()

valid_statuses = ["DELIVERED", "CANCELLED"]
clean_orders = (
    orders_raw
    .withColumn("order_status", F.upper(F.trim("order_status")))
    .withColumn("order_total", F.coalesce("order_total", F.lit(0).cast("decimal(12,2)")))
    .filter(F.col("order_total") >= 0)
    .filter(F.col("order_status").isin(valid_statuses))
    .filter(F.col("customer_id").isNotNull())
)

window = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc(), F.col("source_file").desc())
clean_orders = clean_orders.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")

customers = (
    spark.table(f"`{catalog}`.`{bronze}`.`customers`")
    .dropDuplicates(["customer_id"])
    .withColumnRenamed("region", "customer_region")
    .select("customer_id", "first_name", "email", "phone", "customer_region")
)
restaurants = (
    spark.table(f"`{catalog}`.`{bronze}`.`restaurants`")
    .dropDuplicates(["restaurant_id"])
    .withColumnRenamed("region", "restaurant_region")
    .select("restaurant_id", "restaurant_name", "restaurant_region", "category")
)
drivers = (
    spark.table(f"`{catalog}`.`{bronze}`.`drivers`")
    .dropDuplicates(["driver_id"])
    .withColumnRenamed("region", "driver_region")
    .withColumnRenamed("email", "driver_email")
    .select("driver_id", "driver_email", "driver_region", "base_fee")
)

enriched = (
    clean_orders.join(customers, "customer_id", "left")
    .join(restaurants, "restaurant_id", "left")
    .join(drivers, "driver_id", "left")
    # Canonical operational region used by Gold and the row filter.
    .withColumn("region", F.col("customer_region"))
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("is_late", F.col("delivery_minutes") > 45)
)

enriched.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{silver}`.`orders`")
customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{silver}`.`customers`")
restaurants.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{silver}`.`restaurants`")
drivers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{silver}`.`drivers`")

after = enriched.count()
print(f"Silver metrics: bronze_rows={before}, silver_rows={after}, rejected_or_deduplicated={before - after}")
