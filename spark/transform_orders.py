from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path


# PostgreSQL JDBC driver
jar_path = "/opt/airflow/jars/postgresql-42.7.7.jar"


# Create Spark session
spark = (
    SparkSession.builder
    .appName("EcommerceTransformations")
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


# Read source tables
orders = spark.read.jdbc(
    jdbc_url,
    "orders",
    properties=properties
)

customers = spark.read.jdbc(
    jdbc_url,
    "customers",
    properties=properties
)

products = spark.read.jdbc(
    jdbc_url,
    "products",
    properties=properties
)


# Transform data
result = (
    orders
    .join(customers, "customer_id", "left")
    .join(products, "product_id", "left")
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    )
)


# Select Silver columns
silver_orders = result.select(
    "order_id",
    "customer_id",
    "name",
    "product_id",
    "product_name",
    "quantity",
    "price",
    "order_date",
    "total_amount"
)


# Show transformed data
silver_orders.show()


# Write Silver data to PostgreSQL
silver_orders.write.jdbc(
    jdbc_url,
    "silver_orders",
    mode="overwrite",
    properties=properties
)


print("Silver data loaded successfully!")


# Stop Spark
spark.stop()
