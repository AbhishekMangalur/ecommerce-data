from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, round


# PostgreSQL JDBC driver
jar_path = "/opt/airflow/jars/postgresql-42.7.7.jar"


# Create Spark session
spark = (
    SparkSession.builder
    .appName("EcommerceGoldLayer")
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
silver_orders = spark.read.jdbc(
    jdbc_url,
    "silver_orders",
    properties=properties
)


# Create customer-level sales summary
gold_customer_sales = (
    silver_orders
    .groupBy(
        "customer_id",
        "name"
    )
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("total_amount"), 2).alias("total_spent")
    )
)


# Show Gold data
gold_customer_sales.show()


# Write Gold table to PostgreSQL
gold_customer_sales.write.jdbc(
    jdbc_url,
    "gold_customer_sales",
    mode="overwrite",
    properties=properties
)


print("Gold customer sales table loaded successfully!")


# Stop Spark
spark.stop()
