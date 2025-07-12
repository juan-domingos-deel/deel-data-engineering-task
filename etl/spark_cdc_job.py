from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StructField, StringType

# Start Spark session
spark = SparkSession.builder \
    .appName("CDC-Open-Orders-Aggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Debezium payload schema (simplified for 'after')
schema = StructType([
    StructField("after", StructType([
        StructField("order_id", StringType()),
        StructField("delivery_date", StringType()),
        StructField("status", StringType())
    ]))
])

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pgserver1.public.orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse Debezium JSON
value_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.after.*")

# Filter and aggregate
agg_df = value_df.filter(col("status") == "PENDING") \
    .groupBy("delivery_date") \
    .agg(count("*").alias("open_order_count"))

# Output to console
agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
