from pyspark.sql import SparkSession

# 1. Start Spark in local mode: 2 worker threads on this machine
spark = (SparkSession.builder
         .appName("ecommerce-explore")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")        # hide the noisy INFO logs

DATA = "/home/jovyan/work/data"               # the data folder as the container sees it


def read_json(name):
    # Our files are ONE big JSON array, not one record per line,
    # so Spark needs multiLine=True to parse them.
    return spark.read.option("multiLine", True).json(f"{DATA}/{name}")


# 2. The small files: row count + the schema Spark worked out by itself
for name in ["categories.json", "products.json", "users.json", "transactions.json"]:
    df = read_json(name)
    print(f"\n===== {name}: {df.count():,} rows =====")
    df.printSchema()

# 3. One sessions file (100,000 sessions, about 188 MB)
sessions = read_json("sessions_0.json")
print(f"\n===== sessions_0.json: {sessions.count():,} rows =====")
sessions.printSchema()

spark.stop()