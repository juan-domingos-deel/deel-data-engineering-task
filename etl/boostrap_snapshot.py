from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bootstrap-Orders") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

delta_path = "/tmp/open_orders"

# Step 2: Connect to the source PostgreSQL database
jdbc_url = "jdbc:postgresql://localhost:5432/finance_db"
jdbc_props = {
    "user": "finance_db_user",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

snapshot_df = spark.read.jdbc(url=jdbc_url, table="operations.orders", properties=jdbc_props)

# Step 3: Filter and aggregate open orders
open_orders_df = orders_df.filter(col("status") == "PENDING") \
    .groupBy("delivery_date", "status") \
    .agg(count("*").alias("open_order_count"))

# Step 4: Write the results to a PostgreSQL analytics database
target_url = "jdbc:postgresql://localhost:5432/finance_db"
target_props = {
    "user": "finance_db_user",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

open_orders_df.write.jdbc(
    url=target_url,
        table="monitoring.analytics_open_orders",
    mode="overwrite",  # Replace the table with each run
    properties=target_props
)