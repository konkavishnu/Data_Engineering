from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import from_json, col, unix_timestamp

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Update with your Kafka broker address
USER_CLICK_TOPIC = "user_click"
USER_CLICK_TABLE = "user_click_events"

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Process User Click Events") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define Schema for User Click Events
user_click_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("page", StringType(), True),
    StructField("device", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", LongType(), True)
])

# Step 1: Read Data from Kafka Topic
print("Reading data from Kafka topic...")
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", USER_CLICK_TOPIC) \
    .load()

# Step 2: Deserialize JSON and Apply Schema
print("Deserializing JSON data...")
user_click_df = raw_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), user_click_schema).alias("data")) \
    .select(
    col("data.event_type"),
    col("data.user_id"),
    col("data.page"),
    col("data.device"),
    col("data.location"),
    (col("data.timestamp") / 1000).cast(TimestampType()).alias("event_time")
)

# Step 3: Write Data to Parquet Files with Trigger
print("Writing streaming data to Parquet files...")
checkpoint_dir = "/tmp/spark_checkpoints/user_click"
output_path = "/tmp/spark_output/user_click"

query = user_click_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()