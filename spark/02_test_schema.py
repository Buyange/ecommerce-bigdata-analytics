import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schemas import SESSION_SCHEMA

spark = (SparkSession.builder
         .appName("ecommerce-schema-test")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

DATA = "/home/jovyan/work/data"

start = time.time()
sessions = (spark.read
            .schema(SESSION_SCHEMA)               # our schema: no guessing pass
            .option("multiLine", True)
            .json(f"{DATA}/sessions_0.json"))
print(f"sessions: {sessions.count():,} rows, read in {time.time() - start:.0f}s")
sessions.printSchema()

# explode() turns the map into one row per cart item: (key, value)
print("\nOne row per cart item:")
(sessions
    .filter(F.size("cart_contents") > 0)           # sessions with a non-empty cart
    .select("session_id", "conversion_status", F.explode("cart_contents"))
    .select("session_id", "conversion_status",
            F.col("key").alias("product_id"),
            F.col("value.quantity").alias("quantity"),
            F.col("value.price").alias("price"))
    .show(8, truncate=False))

spark.stop()