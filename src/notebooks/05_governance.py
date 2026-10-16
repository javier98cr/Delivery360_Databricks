# Databricks notebook source
"""Create the stable Delivery360 governance baseline.

This notebook is safe to run without the demo notebook. It is idempotent for
the current user: the current user is configured as an administrator with full
region and PII visibility. The demo notebook can temporarily change these
mappings and then restore this baseline.
"""

dbutils.widgets.text("catalog", "delivery360")
catalog = dbutils.widgets.get("catalog")
governance = "governance"
gold = "gold"

# Which regions is each user allowed to see?
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{governance}`.`user_region_access` (
  user_email STRING,
  allowed_region STRING,
  profile STRING
) USING DELTA
""")

# Is this user allowed to see original email and phone?
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{governance}`.`column_access` (
  user_email STRING,
  profile STRING,
  can_view_pii BOOLEAN
) USING DELTA
""")


spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{governance}`.`profile_access_matrix` (
  profile STRING,
  region_scope STRING,
  can_view_pii BOOLEAN,
  expected_behavior STRING
) USING DELTA
""")

spark.sql(f"DELETE FROM `{catalog}`.`{governance}`.`user_region_access` WHERE user_email = current_user()")
spark.sql(f"DELETE FROM `{catalog}`.`{governance}`.`column_access` WHERE user_email = current_user()")

# ALL REGIONS
spark.sql(f"""
INSERT INTO `{catalog}`.`{governance}`.`user_region_access`
SELECT current_user(), '*', 'admin'
""")
spark.sql(f"""
INSERT INTO `{catalog}`.`{governance}`.`column_access`
SELECT current_user(), 'admin', true
""")

spark.sql(f"DELETE FROM `{catalog}`.`{governance}`.`profile_access_matrix`")
spark.sql(f"""
INSERT INTO `{catalog}`.`{governance}`.`profile_access_matrix`
VALUES
  ('admin', 'All regions', true, 'All rows and original PII values'),
  ('operations', 'Assigned regions', false, 'Only assigned regions; PII masked'),
  ('analyst', 'All regions', false, 'All rows; PII masked')
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION `{catalog}`.`{governance}`.`can_view_region`(row_region STRING)
RETURNS BOOLEAN
RETURN EXISTS (
  SELECT 1 FROM `{catalog}`.`{governance}`.`user_region_access` m
  WHERE m.user_email = current_user()
    AND (m.allowed_region = row_region OR m.allowed_region = '*')
)
""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{catalog}`.`{governance}`.`mask_pii`(value STRING)
RETURNS STRING
RETURN CASE
  WHEN EXISTS (
    SELECT 1 FROM `{catalog}`.`{governance}`.`column_access` m
    WHERE m.user_email = current_user() AND m.can_view_pii = true
  ) THEN value
  WHEN value IS NULL THEN NULL
  ELSE concat(substr(value, 1, 1), '***')
END
""")

spark.sql(f"""
ALTER TABLE `{catalog}`.`{gold}`.`fact_orders`
SET ROW FILTER `{catalog}`.`{governance}`.`can_view_region` ON (region)
""")

for table in ["fact_orders", "dim_customer"]:
    for column in ["email", "phone"]:
        spark.sql(f"""
        ALTER TABLE `{catalog}`.`{gold}`.`{table}`
        ALTER COLUMN `{column}` SET MASK `{catalog}`.`{governance}`.`mask_pii`
        """)
        spark.sql(f"""
        ALTER TABLE `{catalog}`.`{gold}`.`{table}`
        ALTER COLUMN `{column}` SET TAGS ('sensitivity' = 'sensitive', 'data_class' = 'pii')
        """)

print("Governance baseline applied for current_user(): admin, all regions, PII visible")
display(spark.table(f"`{catalog}`.`{governance}`.`profile_access_matrix`"))
