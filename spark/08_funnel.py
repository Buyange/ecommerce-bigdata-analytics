import json
import os
import happybase
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

spark = (SparkSession.builder
         .appName("ecommerce-funnel")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

RESULTS = "/home/jovyan/work/results"
os.makedirs(RESULTS, exist_ok=True)

# ---------------------------------------------------------------- HBase
# Read the browsing sessions from HBase (Thrift server, service name 'hbase').
connection = happybase.Connection("hbase", port=9090, timeout=120000)
table = connection.table("user_sessions")

rows = []
for key, data in table.scan(columns=[b"meta:session_id", b"meta:conversion_status",
                                     b"meta:cart_contents", b"meta:referrer",
                                     b"device:type", b"pv"],       # 'pv' = every page-view column
                            batch_size=1000):
    # each pv:NNNN cell is JSON; keep only the page type of each view
    page_types = [json.loads(v)["page_type"] for col, v in data.items() if col.startswith(b"pv:")]
    rows.append((
        data[b"meta:session_id"].decode(),
        key.decode().split("#")[0],                       # row key starts with user_id
        data[b"device:type"].decode(),
        data[b"meta:referrer"].decode(),
        data[b"meta:conversion_status"].decode(),
        len(json.loads(data[b"meta:cart_contents"])),     # number of products in the cart
        page_types,
    ))
print(f"read {len(rows):,} sessions from HBase")

sessions = spark.createDataFrame(rows, StructType([
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("device_type", StringType()),
    StructField("referrer", StringType()),
    StructField("conversion_status", StringType()),
    StructField("n_cart_items", LongType()),
    StructField("page_types", ArrayType(StringType())),
]))

# -------------------------------------------------------------- MongoDB
# Read the orders that are linked to a session from MongoDB.
mongo = MongoClient("mongodb://mongo:27017")["ecommerce"]
orders = [(d["session_id"], d["_id"], d["status"], float(d["total"]))
          for d in mongo.transactions.find({"session_id": {"$ne": None}},
                                           {"session_id": 1, "status": 1, "total": 1})]
orders_df = spark.createDataFrame(
    orders, "session_id string, transaction_id string, status string, total double")
print(f"read {len(orders):,} session-linked orders from MongoDB")

# ----------------------------------------------------------- Spark join
joined = (sessions.join(orders_df, "session_id", "left")
          .withColumn("s1_viewed_product", F.array_contains("page_types", "product_detail"))
          .withColumn("s2_in_cart", F.col("s1_viewed_product") & (F.col("n_cart_items") > 0))
          .withColumn("s3_checkout", F.col("s2_in_cart") & F.array_contains("page_types", "checkout"))
          .withColumn("s4_purchased", F.col("s3_checkout") & F.col("transaction_id").isNotNull()))
joined.cache()

STAGES = [("1 viewed a product", "s1_viewed_product"),
          ("2 added to cart", "s2_in_cart"),
          ("3 reached checkout", "s3_checkout"),
          ("4 purchased (order in MongoDB)", "s4_purchased")]

# --------------------------------------------------------- overall funnel
total = joined.count()
counts = joined.agg(*[F.sum(F.col(c).cast("int")).alias(c) for _, c in STAGES]).first().asDict()
funnel, previous = [("0 all sessions", total, 100.0, 100.0)], total
for label, col in STAGES:
    n = counts[col]
    funnel.append((label, n, round(100 * n / total, 2), round(100 * n / previous, 2)))
    previous = n
funnel_df = pd.DataFrame(funnel, columns=["stage", "sessions", "pct_of_all", "pct_of_previous"])
print("\nFUNNEL")
print(funnel_df.to_string(index=False))
funnel_df.to_csv(f"{RESULTS}/funnel.csv", index=False)

# ------------------------------------------------------------ by device
by_device = (joined.groupBy("device_type")
             .agg(F.count("*").alias("sessions"),
                  *[F.sum(F.col(c).cast("int")).alias(c) for _, c in STAGES])
             .withColumn("purchase_rate_pct", F.round(100 * F.col("s4_purchased") / F.col("sessions"), 2))
             .orderBy("device_type"))
print("\nFUNNEL BY DEVICE")
by_device.show(truncate=False)
by_device.toPandas().to_csv(f"{RESULTS}/funnel_by_device.csv", index=False)

# ------------------------------------------------- consistency checks
print("CHECK 1: HBase conversion flag vs MongoDB orders")
(joined.groupBy("conversion_status")
       .agg(F.count("*").alias("sessions"),
            F.sum(F.col("transaction_id").isNotNull().cast("int")).alias("with_mongo_order"))
       .orderBy("conversion_status").show())

n_orders = orders_df.count()
print("CHECK 2: session-linked orders:", n_orders,
      "| distinct session_ids:", orders_df.select("session_id").distinct().count())
print("CHECK 3: orders whose session is not in HBase (second session file):",
      orders_df.join(sessions, "session_id", "left_anti").count())

spark.stop()