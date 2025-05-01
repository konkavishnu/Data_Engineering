from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, expr, when, rank
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("News Platform Analytics") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "1000") \
    .getOrCreate()

user_activity_df = spark.read.csv("A:/Data_Engineering/Day287_datasets/new_user_activity.csv", header=True, inferSchema=True)
article_metadata_df = spark.read.csv("A:/Data_Engineering/Day287_datasets/new_user_activity.csv", header=True, inferSchema=True)
user_profile_df = spark.read.csv("A:/Data_Engineering/Day287_datasets/new_user_activity.csv", header=True, inferSchema=True)

user_activity_df.show()
article_metadata_df.show()
user_profile_df.show()

user_activity_df.createOrReplaceTempView("user_activity")
article_metadata_df.createOrReplaceTempView("article_metadata")
user_profile_df.createOrReplaceTempView("user_profile")

user_activity_df = user_activity_df.dropDuplicates(["user_id", "article_id", "timestamp"])


user_activity_df.cache()
user_profile_df.cache()
article_metadata_df.cache()

user_management_df = spark.sql("SELECT user_id,COUNT(CASE WHEN action = 'read' THEN 1 END) AS read_count,"
                               "COUNT(CASE WHEN action = 'like' THEN 1 END) AS like_count,"
                               "COUNT(CASE WHEN action = 'share' THEN 1 END) AS share_count,"
                               "CASE "
                               "WHEN COUNT(action) >= 100 THEN 'Highly Engaged'"
                               "WHEN COUNT(action) BETWEEN 50 AND 99 THEN 'Moderately Engaged'"
                               "ELSE 'Low Engagement'"
                               "END AS engagement_level FROM user_activity GROUP BY user_id")

user_management_df.createOrReplaceTempView("user_engagement")

article_engagement_df = user_activity_df.groupBy("article_id") \
    .agg(count(when(col("action") == "read", True)).alias("views"),
         count(when(col("action") == "like", True)).alias("likes"),
         count(when(col("action") == "share", True)).alias("shares"))

article_with_metadata_df = article_engagement_df.join(article_metadata_df, "article_id", "inner")

article_with_metadata_df.show()

# Define a window specification for ranking articles within each category
category_window = Window.partitionBy("category").orderBy(col("views").desc())

# Apply the rank function over the window to assign a rank within each category
ranked_articles_df = article_with_metadata_df.withColumn("rank_in_category", rank().over(category_window))

# Register ranked articles as a temporary view
ranked_articles_df.createOrReplaceTempView("ranked_articles")

# Step 6: Demographic Preferences Analysis

# Explanation:
# - Join user profiles and activities to analyze popular categories by demographic.
# - Aggregates engagement count for each category by region and gender.
demographic_preferences_sql = """
SELECT
    p.region,
    p.gender,
    m.category,
    COUNT(ua.user_id) AS engagement_count
FROM user_profile p
JOIN user_activity ua ON p.user_id = ua.user_id
JOIN article_metadata m ON ua.article_id = m.article_id
GROUP BY p.region, p.gender, m.category
ORDER BY p.region, p.gender, engagement_count DESC
"""

demographic_preferences_df = spark.sql(demographic_preferences_sql)
demographic_preferences_df.createOrReplaceTempView("demographic_preferences")

# Step 7: Advanced Aggregation - Average Time Spent by User per Category

# Explanation:
# - `.groupBy(...).agg(...)` calculates the total time spent on each article.
# - Join with article metadata to categorize time spent per user.
user_time_spent_df = user_activity_df.groupBy("user_id", "article_id") \
    .agg(sum("time_spent").alias("total_time_spent"))

user_time_spent_df.createOrReplaceTempView("user_time_spent")

avg_time_spent_sql = """
SELECT
    u.user_id,
    m.category,
    AVG(t.total_time_spent) AS avg_time_spent
FROM user_time_spent t
JOIN article_metadata m ON t.article_id = m.article_id
JOIN user_profile u ON t.user_id = u.user_id
GROUP BY u.user_id, m.category
ORDER BY avg_time_spent DESC
"""

avg_time_spent_df = spark.sql(avg_time_spent_sql)
avg_time_spent_df.createOrReplaceTempView("avg_time_spent")

# Step 8: Save DataFrames as Permanent Tables

# Explanation:
# - `.write.saveAsTable(...)` saves DataFrames as managed tables in Spark’s catalog for future querying.
article_with_metadata_df.write.saveAsTable("user_engagement_segmented")
ranked_articles_df.write.saveAsTable("ranked_article_popularity")
demographic_preferences_df.write.saveAsTable("demographic_preferences")
avg_time_spent_df.write.saveAsTable("avg_time_spent_per_user")

# Step 9: Export Results to External Files

# Explanation:
# - `.write.csv(...)` and `.write.json(...)` save results to external files in specified formats.
article_with_metadata_df.write.csv("user_engagement.csv", header=True, mode="overwrite")
ranked_articles_df.write.json("ranked_articles.json", mode="overwrite")
demographic_preferences_df.write.csv("demographic_preferences.csv", header=True, mode="overwrite")

# Step 10: Stop Spark Session

# Explanation:
# - `spark.stop()` releases all resources and stops the Spark session to prevent resource locking.
spark.stop()




