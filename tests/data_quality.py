from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# PostgreSQL JDBC driver
jar_path = "/opt/airflow/jars/postgresql-42.7.7.jar"


# Create Spark session
spark = (
    SparkSession.builder
    .appName("EcommerceDataQuality")
    .master("local[*]")
    .config("spark.driver.extraClassPath", jar_path)
    .config("spark.executor.extraClassPath", jar_path)
    .config(
        "spark.driver.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .config(
        "spark.executor.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .getOrCreate()
)


# PostgreSQL connection
jdbc_url = "jdbc:postgresql://host.docker.internal:5432/ecommerce"

properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}


# Read Silver table
df = spark.read.jdbc(
    jdbc_url,
    "silver_orders",
    properties=properties
)


# -----------------------------
# Data Quality Checks
# -----------------------------

# 1. Check NULL values
null_count = (
    df.filter(
        col("order_id").isNull()
        | col("customer_id").isNull()
        | col("product_id").isNull()
        | col("quantity").isNull()
        | col("price").isNull()
    )
    .count()
)

print(f"NULL check: {null_count} invalid rows")


# 2. Check quantity > 0
invalid_quantity = (
    df.filter(col("quantity") <= 0)
    .count()
)

print(f"Quantity check: {invalid_quantity} invalid rows")


# 3. Check price >= 0
invalid_price = (
    df.filter(col("price") < 0)
    .count()
)

print(f"Price check: {invalid_price} invalid rows")


# 4. Check total_amount >= 0
invalid_total = (
    df.filter(col("total_amount") < 0)
    .count()
)

print(f"Total amount check: {invalid_total} invalid rows")


# 5. Check duplicate order IDs
duplicate_orders = (
    df.groupBy("order_id")
    .agg(count("*").alias("count"))
    .filter(col("count") > 1)
    .count()
)

print(f"Duplicate order check: {duplicate_orders} duplicate orders")


# -----------------------------
# Final Result
# -----------------------------

total_errors = (
    null_count
    + invalid_quantity
    + invalid_price
    + invalid_total
    + duplicate_orders
)


if total_errors == 0:
    print("DATA QUALITY CHECK PASSED")
else:
    print(f"DATA QUALITY CHECK FAILED: {total_errors} errors")


spark.stop()
