from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, row_number, from_unixtime, expr, to_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("CDC-Stream-Into-Delta") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

delta_path = "/tmp/open_orders"

# Debezium schema
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

payload_schema = StructType([
    StructField("payload", StructType([
        StructField("after", after_schema),
        StructField("op", StringType())
    ]))
])

# Read Kafka CDC stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "finance_db.operations.orders") \
    .option("startingOffsets", "latest") \
    .load()

# Extract and parse Debezium JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), payload_schema).alias("data")) \
    .select("data.payload.after.*", "data.payload.op") \
    .filter(col("op").isin("c", "u")) \
    .drop("op") \
    .withColumnRenamed("updated_at", "updated_at_raw") \
    .withColumn("updated_at", to_utc_timestamp(from_unixtime(col("updated_at_raw") / 1000), "UTC")) \
    .withColumnRenamed("created_at", "created_at_raw") \
    .withColumn("created_at", to_utc_timestamp(from_unixtime(col("created_at_raw") / 1000), "UTC")) \
    .withColumn("order_date", expr("date_add('1970-01-01', order_date)")) \
    .withColumn("delivery_date", expr("date_add('1970-01-01', delivery_date)"))

# Per-batch logic to deduplicate and merge
from delta.tables import DeltaTable

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n==== Batch {batch_id} ====")

    window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

    deduped = batch_df.withColumn("rn", row_number().over(window_spec)) \
        .filter("rn = 1") \
        .drop("rn")

    if DeltaTable.isDeltaTable(spark, delta_path):
        delta = DeltaTable.forPath(spark, delta_path)
        delta.alias("tgt").merge(
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
        ).whenNotMatchedInsertAll().execute()
    else:
        deduped.write.format("delta").mode("overwrite").save(delta_path)

    # Optional: print current snapshot summary
    full_state = spark.read.format("delta").load(delta_path)
    agg_df = full_state.filter(col("status") != "COMPLETED") \
        .groupBy("delivery_date", "status") \
        .agg(count("*").alias("open_order_count"))

    agg_df.orderBy("delivery_date", "status").show(truncate=False)

# Start streaming job
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
