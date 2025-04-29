from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, ArrayType
from pyspark.sql.functions import from_json, col, explode

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
ORDER_DETAILS_TOPIC = "order_details_topic"

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Process Order Details Events") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define Schema for Order Details Events
order_items_schema = StructType([
    StructField("item_id", LongType(), True),
    StructField("item_name", StringType(), True),
    StructField("price", DoubleType(), True)
])

order_details_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("items", ArrayType(order_items_schema), True)
])

# Step 1: Read Data from Kafka Topic
print("Reading data from Kafka topic...")
raw_order_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ORDER_DETAILS_TOPIC) \
    .load()

# Step 2: Deserialize JSON and Apply Schema
print("Deserializing JSON data...")
order_details_df = raw_order_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_details_schema).alias("data")) \
    .select(
    col("data.event_type"),
    col("data.order_id"),
    col("data.user_id"),
    col("data.order_amount"),
    col("data.transaction_id"),
    col("data.payment_method"),
    (col("data.timestamp") / 1000).cast(TimestampType()).alias("event_time"),
    col("data.items")
)

# Step 3: Flatten the Items Array for Detailed Analysis
print("Flattening the items array...")
flattened_order_details_df = order_details_df \
    .withColumn("item", explode(col("items"))) \
    .select(
    col("event_type"),
    col("order_id"),
    col("user_id"),
    col("order_amount"),
    col("transaction_id"),
    col("payment_method"),
    col("event_time"),
    col("item.item_id").alias("item_id"),
    col("item.item_name").alias("item_name"),
    col("item.price").alias("item_price")
)

# Step 4: Write Data to Parquet Files with Trigger
print("Writing streaming data to Parquet files...")
order_checkpoint_dir = "/tmp/spark_checkpoints/order_details"
order_output_path = "/tmp/spark_output/order_details"

order_query = flattened_order_details_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", order_output_path) \
    .option("checkpointLocation", order_checkpoint_dir) \
    .trigger(processingTime="60 seconds") \
    .start()

order_query.awaitTermination()