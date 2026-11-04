from pyspark.sql import SparkSession, functions as F
from schemas import SESSION_SCHEMA

spark = (SparkSession.builder
         .appName("ecommerce-clean")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

DATA = "/home/jovyan/work/data"
OUT = f"{DATA}/clean"                           # cleaned Parquet files go here
REVENUE_STATUSES = ["completed", "shipped", "delivered"]


def read_json(name, schema=None):
    reader = spark.read.option("multiLine", True)
    if schema:
        reader = reader.schema(schema)
    return reader.json(f"{DATA}/{name}")


def clean(df, table, key, rules):
    """Count violations of each rule in ONE pass, print a report,
    then drop invalid rows and duplicate keys."""
    aggs = [F.count(F.lit(1)).alias("rows")]
    valid_all = F.lit(True)
    for name, cond in rules:
        ok = F.coalesce(cond, F.lit(False))           # a NULL result counts as invalid
        aggs.append(F.sum(F.when(~ok, 1).otherwise(0)).alias(name))
        valid_all = valid_all & ok
    stats = df.agg(*aggs).first().asDict()
    print(f"\n[{table}] raw rows: {stats.pop('rows'):,}")
    for name, bad in stats.items():
        print(f"    rule '{name}': {bad:,} rows violate it")
    return df.filter(valid_all).dropDuplicates([key])


def save(df, name):
    path = f"{OUT}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"saved {name}: {spark.read.parquet(path).count():,} rows")


# ------------------------------------------------------------------ users
users = clean(read_json("users.json"), "users", "user_id", [
    ("user_id not null", F.col("user_id").isNotNull()),
    ("registration_date parses", F.to_timestamp("registration_date").isNotNull()),
])
users = (users
         .withColumn("registration_date", F.to_timestamp("registration_date"))
         .withColumn("last_active", F.to_timestamp("last_active"))
         .withColumn("registration_month", F.date_format("registration_date", "yyyy-MM")))
save(users, "users")

# --------------------------------------------------------------- products
products = clean(read_json("products.json"), "products", "product_id", [
    ("product_id not null", F.col("product_id").isNotNull()),
    ("base_price > 0", F.col("base_price") > 0),
    ("current_stock >= 0", F.col("current_stock") >= 0),
])
products = (products
            .withColumn("creation_date", F.to_timestamp("creation_date"))
            # turn the date strings inside the price_history list into timestamps
            .withColumn("price_history", F.transform(
                "price_history",
                lambda h: F.struct(h["price"].alias("price"),
                                   F.to_timestamp(h["date"]).alias("date")))))
save(products, "products")

# ----------------------------------------------------------- transactions
transactions = clean(read_json("transactions.json"), "transactions", "transaction_id", [
    ("ids not null", F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull()),
    ("total >= 0", F.col("total") >= 0),
    ("has items", F.size("items") > 0),
    ("total = subtotal - discount",
     F.abs(F.col("total") - (F.col("subtotal") - F.col("discount"))) <= 0.01),
])
transactions = (transactions
                .withColumn("timestamp", F.to_timestamp("timestamp"))
                .withColumn("txn_date", F.to_date("timestamp"))
                .withColumn("is_revenue", F.col("status").isin(REVENUE_STATUSES)))
save(transactions, "transactions")

# One row per purchased line item (easier to group and join than a nested list)
transaction_items = (transactions
    .select("transaction_id", "user_id", "session_id", "timestamp", "txn_date",
            "is_revenue", F.explode("items").alias("it"))
    .select("transaction_id", "user_id", "session_id", "timestamp", "txn_date", "is_revenue",
            F.col("it.product_id").alias("product_id"),
            F.col("it.quantity").alias("quantity"),
            F.col("it.unit_price").alias("unit_price"),
            F.col("it.subtotal").alias("subtotal")))
save(transaction_items, "transaction_items")

# --------------------------------------------------------------- sessions
# Both session files at once (200,000 sessions); the explicit schema avoids a guessing pass.
sessions = clean(read_json("sessions_*.json", SESSION_SCHEMA), "sessions", "session_id", [
    ("ids not null", F.col("session_id").isNotNull() & F.col("user_id").isNotNull()),
    ("start_time parses", F.to_timestamp("start_time").isNotNull()),
    ("end_time >= start_time", F.to_timestamp("end_time") >= F.to_timestamp("start_time")),
    ("duration_seconds > 0", F.col("duration_seconds") > 0),
])
sessions = (sessions
            .withColumn("start_time", F.to_timestamp("start_time"))
            .withColumn("end_time", F.to_timestamp("end_time"))
            .withColumn("start_date", F.to_date("start_time")))

# Stage the cleaned sessions once, then derive the three session tables from the Parquet copy
save(sessions, "sessions_staged")
staged = spark.read.parquet(f"{OUT}/sessions_staged")

session_facts = staged.select(
    "session_id", "user_id", "start_time", "end_time", "start_date", "duration_seconds",
    F.col("geo_data.country").alias("country"), F.col("geo_data.state").alias("state"),
    F.col("geo_data.city").alias("city"),
    F.col("device_profile.type").alias("device_type"), F.col("device_profile.os").alias("os"),
    F.col("device_profile.browser").alias("browser"),
    "referrer", "conversion_status",
    F.size("page_views").alias("n_page_views"),
    F.size("cart_contents").alias("n_cart_items"))
save(session_facts, "sessions")

page_views = (staged
    .select("session_id", "user_id", "start_date", F.posexplode("page_views").alias("pos", "pv"))
    .select("session_id", "user_id", "start_date",
            (F.col("pos") + 1).alias("step"),                       # position inside the session
            F.to_timestamp("pv.timestamp").alias("ts"),
            F.col("pv.page_type").alias("page_type"),
            F.col("pv.product_id").alias("product_id"),
            F.col("pv.category_id").alias("category_id"),
            F.col("pv.view_duration").alias("view_duration")))
save(page_views, "session_page_views")

cart_items = (staged
    .select("session_id", "user_id", "start_date", "conversion_status",
            F.explode("cart_contents").alias("product_id", "item"))   # map -> key, value
    .select("session_id", "user_id", "start_date", "conversion_status", "product_id",
            F.col("item.quantity").alias("quantity"),
            F.col("item.price").alias("price")))
save(cart_items, "session_cart_items")

spark.stop()