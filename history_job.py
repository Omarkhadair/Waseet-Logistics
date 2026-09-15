from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("waseet-history")
         .master("local[*]")
         .getOrCreate())

# TODO. An explicit schema.
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
SCHEMA = StructType([
    StructField("scan_id", StringType(), True),
    StructField("parcel_id", StringType(), True),
    StructField("hub_id", StringType(), True),
    StructField("scan_type", StringType(), True),
    StructField("scanned_at", StringType(), True),
    StructField("weight_kg", StringType(), True),
    StructField("courier_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("service_level_id", StringType(), True),
    StructField("service_code", StringType(), True)
])

# TODO. Read data/scans_history.csv with that schema and header=True.
history = spark.read.option("header", True).schema(SCHEMA).csv("../data/scans_history.csv")

from pyspark.sql.functions import broadcast, col, to_timestamp, to_date

hubs = spark.read.option("header", True).csv("../data/hubs.csv")
service_levels = spark.read.option("header", True).csv("../data/service_levels.csv")

shaped = history \
    .withColumn("scanned_at", to_timestamp("scanned_at")) \
    .withColumn("scan_date", to_date("scanned_at")) \
    .join(broadcast(hubs), "hub_id", "left") \
    .join(broadcast(service_levels), "service_level_id", "left") \
    .drop("customer_id", "courier_id") 
# TODO. Aggregate. At least these two, and read the plan for both:
from pyspark.sql import functions as F

scans_per_hub_day = shaped.groupBy("hub_id", "scan_date") \
    .agg(F.count("scan_id").alias("total_scans"))

region_monthly_share = shaped \
    .withColumn("month", F.date_format("scan_date", "yyyy-MM")) \
    .groupBy("region", "month") \
    .agg(
        F.sum(F.when(F.col("scan_type") == "delivered", 1).otherwise(0)).alias("delivered_count"),
        F.sum(F.when(F.col("scan_type") == "failed", 1).otherwise(0)).alias("failed_count"),
        F.count("scan_id").alias("total_scans")
    ) \
    .withColumn("delivered_share", F.col("delivered_count") / F.col("total_scans")) \
    .withColumn("failed_share", F.col("failed_count") / F.col("total_scans"))

# TODO. Write the curated zone as Parquet, partitioned by scan_date.
output_path = "../data/curated_scans"

shaped.write \
    .mode("overwrite") \
    .partitionBy("scan_date") \
    .parquet(output_path)

print(f"Curated zone written successfully to {output_path}")

spark.stop()
