from pyspark.sql import SparkSession, functions as F
from schemas import SESSION_SCHEMA

spark = (SparkSession.builder
         .appName("ecommerce-quality-check")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

DATA = "/home/jovyan/work/data"


def read_json(name, schema=None):
    reader = spark.read.option("multiLine", True)
    if schema:
        reader = reader.schema(schema)
    return reader.json(f"{DATA}/{name}")


def section(title):
    print(f"\n========== {title} ==========")


def null_counts(df, columns):
    """One-row table: how many nulls in each listed column."""
    df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).show()


users = read_json("users.json")
products = read_json("products.json")
transactions = read_json("transactions.json")
sessions = read_json("sessions_0.json", SESSION_SCHEMA)

# ---------------------------------------------------------------- users
section("USERS")
print("rows:", users.count(), "| distinct user_id:", users.select("user_id").distinct().count())
null_counts(users, ["user_id", "registration_date", "last_active"])

# ------------------------------------------------------------- products
section("PRODUCTS")
print("rows:", products.count(), "| distinct product_id:", products.select("product_id").distinct().count())
print("base_price <= 0:", products.filter("base_price <= 0").count())
print("current_stock < 0:", products.filter("current_stock < 0").count())
print("inactive products:", products.filter("is_active = false").count())

# --------------------------------------------------------- transactions
section("TRANSACTIONS")
t = transactions
print("rows:", t.count(), "| distinct transaction_id:", t.select("transaction_id").distinct().count())
null_counts(t, ["transaction_id", "user_id", "session_id", "timestamp", "total", "status", "payment_method"])
t.groupBy("status").count().orderBy(F.desc("count")).show()
t.groupBy("payment_method").count().orderBy(F.desc("count")).show()
print("total < 0:", t.filter("total < 0").count())
print("total != subtotal - discount:",
      t.filter(F.abs(F.col("total") - (F.col("subtotal") - F.col("discount"))) > 0.01).count())
print("no items:", t.filter(F.size("items") == 0).count())
print("user_id not in users:", t.join(users.select("user_id"), "user_id", "left_anti").count())

items = t.select("transaction_id", F.explode("items").alias("it"))       # one row per line item
print("line items:", items.count())
print("item quantity <= 0:", items.filter("it.quantity <= 0").count())
print("item unit_price * quantity != subtotal:",
      items.filter(F.abs(F.col("it.unit_price") * F.col("it.quantity") - F.col("it.subtotal")) > 0.01).count())
print("item product_id not in products:",
      items.join(products.select(F.col("product_id").alias("pid")),
                 F.col("it.product_id") == F.col("pid"), "left_anti").count())

# ------------------------------------------------------------- sessions
section("SESSIONS (sessions_0.json)")
s = (sessions
     .withColumn("start_ts", F.to_timestamp("start_time"))
     .withColumn("end_ts", F.to_timestamp("end_time")))
print("rows:", s.count(), "| distinct session_id:", s.select("session_id").distinct().count())
null_counts(s, ["session_id", "user_id", "start_time", "end_time", "duration_seconds",
                "conversion_status", "referrer"])
print("end before start:", s.filter(F.col("end_ts") < F.col("start_ts")).count())
print("duration_seconds <= 0:", s.filter("duration_seconds <= 0").count())
print("duration_seconds differs from end - start by > 1s:",
      s.filter(F.abs(F.col("duration_seconds")
                     - (F.unix_timestamp("end_ts") - F.unix_timestamp("start_ts"))) > 1).count())
print("empty page_views:", s.filter(F.size("page_views") == 0).count())
print("user_id not in users:", s.join(users.select("user_id"), "user_id", "left_anti").count())
s.groupBy("conversion_status").count().orderBy(F.desc("count")).show()
s.groupBy("referrer").count().orderBy(F.desc("count")).show()
s.groupBy(F.col("device_profile.type").alias("device_type")).count().orderBy(F.desc("count")).show()

spark.stop()