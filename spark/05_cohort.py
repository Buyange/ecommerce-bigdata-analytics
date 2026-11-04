import os
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("ecommerce-cohort")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

CLEAN = "/home/jovyan/work/data/clean"
RESULTS = "/home/jovyan/work/results"            # small CSV outputs, safe to commit to Git
os.makedirs(RESULTS, exist_ok=True)

users = spark.read.parquet(f"{CLEAN}/users")
# only orders that count as revenue (completed, shipped, delivered)
txns = spark.read.parquet(f"{CLEAN}/transactions").filter("is_revenue")

# 1. Cohort sizes: how many users registered in each month
cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_users"))
print("\nCOHORT SIZES")
cohort_size.orderBy("registration_month").show()

# 2. Attach each purchase to its buyer's cohort
purchases = (txns
    .join(users.select("user_id", "registration_month", "registration_date"), "user_id")
    .withColumn("purchase_month", F.date_format("timestamp", "yyyy-MM"))
    # whole months between the start of the registration month and the start of the purchase month
    .withColumn("months_since_registration",
                F.months_between(F.date_trunc("month", "timestamp"),
                                 F.date_trunc("month", "registration_date")).cast("int")))

# 3. Per cohort and purchase month: buyers, orders, revenue; then normalise by cohort size
cohort = (purchases
    .groupBy("registration_month", "purchase_month", "months_since_registration")
    .agg(F.countDistinct("user_id").alias("active_buyers"),
         F.count("*").alias("orders"),
         F.round(F.sum("total"), 2).alias("revenue"))
    .join(cohort_size, "registration_month")
    .withColumn("revenue_per_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
    .withColumn("buyer_rate", F.round(F.col("active_buyers") / F.col("cohort_users"), 3))
    .orderBy("registration_month", "purchase_month"))

print("COHORT TABLE (first 40 rows)")
cohort.show(40, truncate=False)

# 4. Save the small result as a CSV for the visualisation step later
cohort.toPandas().to_csv(f"{RESULTS}/cohort.csv", index=False)
print("saved results/cohort.csv")

spark.stop()