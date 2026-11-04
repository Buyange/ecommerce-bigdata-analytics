import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("ecommerce-sql")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

CLEAN = "/home/jovyan/work/data/clean"
RESULTS = "/home/jovyan/work/results"
os.makedirs(RESULTS, exist_ok=True)

# Register each Parquet table as a SQL view. The comments say which system would
# hold this data in the full architecture; Spark can join them once loaded.
views = {
    "users": "users",                    # MongoDB: user profiles
    "products": "products",              # MongoDB: product catalogue
    "transactions": "transactions",      # MongoDB: orders
    "txn_items": "transaction_items",    # MongoDB: order line items (flattened)
    "sessions": "sessions",              # HBase: session time series
    "page_views": "session_page_views",  # HBase: page-view events
}
for view, folder in views.items():
    spark.read.parquet(f"{CLEAN}/{folder}").createOrReplaceTempView(view)


def run(title, sql, save_as=None):
    print(f"\n===== {title} =====")
    df = spark.sql(sql)
    df.show(30, truncate=False)
    if save_as:
        df.toPandas().to_csv(f"{RESULTS}/{save_as}.csv", index=False)


# ---- Query 1: top 5 categories per month and their share of that month's revenue
run("Q1: top categories per month (full months only)", """
    WITH monthly AS (
        SELECT p.category_id,
               date_format(t.txn_date, 'yyyy-MM') AS month,
               ROUND(SUM(t.subtotal), 2)          AS revenue
        FROM txn_items t
        JOIN products p ON t.product_id = p.product_id
        WHERE t.is_revenue
        GROUP BY p.category_id, date_format(t.txn_date, 'yyyy-MM')
    ),
    ranked AS (
        SELECT category_id, month, revenue,
               ROUND(100 * revenue / SUM(revenue) OVER (PARTITION BY month), 2) AS pct_of_month,
               RANK() OVER (PARTITION BY month ORDER BY revenue DESC)           AS rank_in_month
        FROM monthly
        WHERE month IN ('2026-09', '2026-10')
    )
    SELECT * FROM ranked WHERE rank_in_month <= 5 ORDER BY month, rank_in_month
""", save_as="sql_top_categories")

# ---- Query 2: top 3 products by revenue inside each category
run("Q2: top 3 products per category (cat_000, cat_001, cat_002)", """
    WITH product_rev AS (
        SELECT p.category_id, p.product_id, p.name,
               ROUND(SUM(t.subtotal), 2) AS revenue,
               SUM(t.quantity)           AS units
        FROM txn_items t
        JOIN products p ON t.product_id = p.product_id
        WHERE t.is_revenue
        GROUP BY p.category_id, p.product_id, p.name
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rn
        FROM product_rev
    )
    SELECT category_id, rn, product_id, name, revenue, units
    FROM ranked
    WHERE rn <= 3 AND category_id IN ('cat_000', 'cat_001', 'cat_002')
    ORDER BY category_id, rn
""")

# ---- Query 3: session engagement (HBase-style data) vs purchases (MongoDB-style data)
run("Q3: engagement quartile vs spending", """
    WITH engagement AS (
        SELECT user_id, COUNT(*) AS n_sessions
        FROM sessions
        GROUP BY user_id
    ),
    purchases AS (
        SELECT user_id, COUNT(*) AS orders, SUM(total) AS revenue
        FROM transactions
        WHERE is_revenue
        GROUP BY user_id
    ),
    joined AS (
        SELECT u.user_id, e.n_sessions,
               COALESCE(p.orders, 0)  AS orders,
               COALESCE(p.revenue, 0) AS revenue,
               NTILE(4) OVER (ORDER BY e.n_sessions) AS engagement_quartile
        FROM users u
        JOIN engagement e ON u.user_id = e.user_id
        LEFT JOIN purchases p ON u.user_id = p.user_id
    )
    SELECT engagement_quartile,
           COUNT(*)                   AS users,
           MIN(n_sessions)            AS min_sessions,
           MAX(n_sessions)            AS max_sessions,
           ROUND(AVG(orders), 2)      AS avg_orders,
           ROUND(AVG(revenue), 2)     AS avg_revenue
    FROM joined
    GROUP BY engagement_quartile
    ORDER BY engagement_quartile
""", save_as="sql_engagement_quartiles")

spark.stop()