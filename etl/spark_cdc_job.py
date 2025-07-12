from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import expr

# Start Spark session
spark = SparkSession.builder \
    .appName("CDC-Open-Orders-Aggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Debezium payload schema (simplified for 'after')
# schema = StructType([
#     StructField("after", StructType([
#         StructField("order_id", StringType()),
#         StructField("delivery_date", StringType()),
#         StructField("status", StringType())
#     ]))
# ])
# Define schema for the 'after' payload
after_schema = StructType([
    StructField("order_id", LongType()),
    StructField("order_date", IntegerType()),
    StructField("delivery_date", IntegerType()),
    StructField("customer_id", LongType()),
    StructField("status", StringType()),
    StructField("updated_at", IntegerType()),
    StructField("updated_by", StringType()),
    StructField("created_at", IntegerType()),
    StructField("created_by", StringType())
])

# Schema for entire Kafka JSON
payload_schema = StructType([
    StructField("payload", StructType([
        StructField("after", after_schema)
    ]))
])

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "finance_db.operations.orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse Debezium JSON
value_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), payload_schema).alias("data")) \
    .select("data.payload.after.*")

# Filter and aggregate
agg_df = value_df.filter(col("status") != "COMPLETED") \
    .groupBy("delivery_date", "status") \
    .agg(count("*").alias("open_order_count"))

parsed_df = agg_df.withColumn(
    "delivery_date", expr("date_add('1970-01-01', delivery_date)")
)

# Output to console
parsed_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
