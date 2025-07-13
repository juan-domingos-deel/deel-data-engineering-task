from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, row_number, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import expr
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder \
    .appName("CDC-Open-Orders-Aggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for the 'after' payload
after_schema = StructType([
    StructField("order_id", LongType()),
    StructField("order_date", IntegerType()),
    StructField("delivery_date", IntegerType()),
    StructField("customer_id", LongType()),
    StructField("status", StringType()),
    StructField("updated_at", LongType()),
    StructField("updated_by", StringType()),
    StructField("created_at", LongType()),
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

parsed_df = value_df \
    .withColumn("order_date", expr("date_add('1970-01-01', order_date)")) \
    .withColumn("delivery_date", expr("date_add('1970-01-01', delivery_date)")) \
    .withColumn("updated_at", from_unixtime(col("updated_at") / 1000).cast("timestamp")) \
    .withColumn("created_at", from_unixtime(col("created_at") / 1000).cast("timestamp"))

def process_batch(batch_df, batch_id):
    window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

    deduped = batch_df.withColumn("rn", row_number().over(window_spec)) \
        .filter("rn = 1") \
        .drop("rn")

    agg_df = deduped.filter(col("status") != "COMPLETED") \
        .groupBy("delivery_date", "status") \
        .agg(count("*").alias("open_order_count"))

    agg_df.orderBy("delivery_date", "status").show(truncate=False)  # or write to sink (JDBC, Delta, etc.)


# Output to console
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="1 second") \
    .start()

query.awaitTermination()
