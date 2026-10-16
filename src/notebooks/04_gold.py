# Databricks notebook source
"""Build the Delivery360 star schema and business metrics."""

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "delivery360")
catalog = dbutils.widgets.get("catalog")
silver = "silver"
gold = "gold"

orders = spark.table(f"`{catalog}`.`{silver}`.`orders`")
customers = spark.table(f"`{catalog}`.`{silver}`.`customers`")
restaurants = spark.table(f"`{catalog}`.`{silver}`.`restaurants`")
drivers = spark.table(f"`{catalog}`.`{silver}`.`drivers`")

# For this educational project, the stable source identifiers are also the
# dimension keys. This keeps PK/FK relationships easy to explain.
dim_customer = customers.select(
    F.col("customer_id").alias("customer_key"),
    "customer_id", "first_name", "email", "phone", "customer_region",
)
dim_restaurant = restaurants.select(
    F.col("restaurant_id").alias("restaurant_key"),
    "restaurant_id", "restaurant_name", "restaurant_region", "category",
)
dim_driver = drivers.select(
    F.col("driver_id").alias("driver_key"),
    "driver_id", "driver_email", "driver_region", "base_fee",
)

dim_date = (
    orders.select(F.to_date("order_ts").alias("date")).distinct()
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_week", F.date_format("date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
)

fact_orders = (
    orders.join(dim_customer.select("customer_id", "customer_key"), "customer_id")
    .join(dim_restaurant.select("restaurant_id", "restaurant_key"), "restaurant_id")
    .join(dim_driver.select("driver_id", "driver_key"), "driver_id")
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .select(
        "order_id", "customer_key", "restaurant_key", "driver_key", "date_key",
        "order_date", "region", "restaurant_name", "email", "phone", "order_status",
        "order_total", "delivery_minutes", "is_late", "source_file",
    )
)

fact_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{gold}`.`fact_orders`")
dim_customer.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{gold}`.`dim_customer`")
dim_restaurant.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{gold}`.`dim_restaurant`")
dim_driver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{gold}`.`dim_driver`")
dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{gold}`.`dim_date`")


# Which regions and restaurants have the most late deliveries and revenue?
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{gold}`.`late_delivery_summary` AS
SELECT region, restaurant_name,
       COUNT(*) AS total_orders,
       SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_orders,
       ROUND(SUM(order_total), 2) AS gross_revenue
FROM `{catalog}`.`{gold}`.`fact_orders`
GROUP BY region, restaurant_name
""")

print("Gold star schema and late_delivery_summary created")
