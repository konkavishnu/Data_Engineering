from pyspark.sql import SparkSession

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Enhance Order Details Data") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 1: Read Data from Hive Tables
print("Reading data from Hive tables...")
user_click_df = spark.sql("SELECT user_id, location FROM user_click")
order_details_df = spark.sql("SELECT * FROM order_details")

# Step 2: Join Data on user_id
print("Joining data...")
order_details_enhanced_df = order_details_df \
    .join(user_click_df, "user_id", "left") \
    .select(
    order_details_df["user_id"],
    user_click_df["location"],
    order_details_df["event_type"],
    order_details_df["order_id"],
    order_details_df["order_amount"],
    order_details_df["transaction_id"],
    order_details_df["payment_method"],
    order_details_df["event_time"],
    order_details_df["item_id"],
    order_details_df["item_name"],
    order_details_df["item_price"]
)

# Step 3: Write Enhanced Data to Hive Table
print("Writing enhanced data to Hive table...")
order_details_enhanced_df.write.mode("overwrite").saveAsTable("order_details_enhanced")

print("Enhanced data written successfully!")