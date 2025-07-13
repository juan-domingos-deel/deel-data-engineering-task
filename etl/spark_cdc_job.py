from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, row_number, to_utc_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import expr
from pyspark.sql.window import Window
import os
from delta.tables import DeltaTable

delta_path = "/tmp/open_orders"

# Start Spark session
spark = SparkSession.builder \
    .appName("CDC-Open-Orders-Aggregation") \
    .config("spark.sql.shuffle.partitions", "1") \
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
        # StructField("op", StringType())
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
    .select("data.payload.after.*") \
    # .filter(col("op").isin("c", "u")) \
    # .drop("op")
print(f"value_df: {value_df}")

parsed_df = value_df \
    .withColumn("order_date", expr("date_add('1970-01-01', order_date)")) \
    .withColumn("delivery_date", expr("date_add('1970-01-01', delivery_date)")) \
    .withColumnRenamed("updated_at", "updated_at_raw") \
    .withColumn("updated_at", to_utc_timestamp(from_unixtime(col("updated_at_raw") / 1000), "UTC")) \
    .withColumnRenamed("created_at", "created_at_raw") \
    .withColumn("created_at", to_utc_timestamp(from_unixtime(col("created_at_raw") / 1000), "UTC"))

def process_batch(batch_df, batch_id):
    print(f"\n==== Batch {batch_id} ====")
    print(f"Input batch count: {batch_df.count()}")
    window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

    deduped = batch_df.withColumn("rn", row_number().over(window_spec)) \
        .filter("rn = 1") \
        .drop("rn")
    print(f"deduped in process batch: {deduped}")
    deduped.select("order_id", "status", "delivery_date", "updated_at").orderBy(col("order_id").desc()).show(truncate=False)
    count_deduped = deduped.count()
    count_deduped_completed = deduped.filter(col("status") == "COMPLETED").count()
    print(f"Deduplicated row count before delta: {count_deduped}")
    print(f"Deduplicated row COMPLETED count before delta: {count_deduped_completed}")
    print(deduped.groupBy("order_id").count().filter("count > 1").show())

# Upsert into Delta table to maintain latest state
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta = DeltaTable.forPath(spark, delta_path)

        (delta.alias("tgt").merge(
            deduped.alias("src"),
            "tgt.order_id = src.order_id"
        ).whenMatchedUpdate(
            condition="src.updated_at > tgt.updated_at",
            set={
                "order_date": "src.order_date",
                "delivery_date": "src.delivery_date",
                "customer_id": "src.customer_id",
                "status": "src.status",
                "updated_at": "src.updated_at",
                "updated_by": "src.updated_by",
                "created_at": "src.created_at",
                "created_by": "src.created_by"
            }
        )
         .whenNotMatchedInsertAll()
         .execute())

    else:
        deduped.write.format("delta").mode("overwrite").save(delta_path)

    # Query the up-to-date state for current open orders
    full_state = spark.read.format("delta").load(delta_path)
    count_full_state = full_state.filter(col("status") != "COMPLETED").count()
    print(f"full_state without COMPLETED: {count_full_state}")

    dropped_duplicates_state = full_state.dropDuplicates(["order_id"])
    print(f"dropped_duplicates_state in process batch: {dropped_duplicates_state}")
    dropped_duplicates_state.select("order_id", "status", "delivery_date", "updated_at").orderBy(col("order_id").desc()).show(truncate=False)
    count_dropped_duplicates_state = dropped_duplicates_state.count()
    print(f"Deduplicated row count after delta: {count_dropped_duplicates_state}")


    agg_df = dropped_duplicates_state.filter(col("status") != "COMPLETED") \
            .groupBy("delivery_date", "status") \
            .agg(count("*").alias("open_order_count"))

    agg_df.orderBy("delivery_date", "status").show(truncate=False)  # or write to sink (JDBC, Delta, etc.)
    print(f"agg_df in process batch: {agg_df}")


# Output to console
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="1 second") \
    .start()

query.awaitTermination()
