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

# snapshot_df = spark.read.jdbc(url=jdbc_url, table="operations.orders", properties=jdbc_props)


# ✅ LongTypeoad full table from Postgres
snapshot_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/finance_db") \
    .option("dbtable", "operations.orders") \
    .option("user", "finance_db_user") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# ✅ Save to Delta (overwrite any existing state)
snapshot_df.write.format("delta").mode("overwrite").save(delta_path)

print("✅ Bootstrapped Delta table with current Postgres state.")
