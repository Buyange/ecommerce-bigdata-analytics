from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, ArrayType, MapType
)

# One page view inside a session
page_view = StructType([
    StructField("timestamp", StringType()),
    StructField("page_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category_id", StringType()),
    StructField("view_duration", LongType()),
])

# One cart line: the value stored under each product_id key
cart_item = StructType([
    StructField("quantity", LongType()),
    StructField("price", DoubleType()),
])

SESSION_SCHEMA = StructType([
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("start_time", StringType()),
    StructField("end_time", StringType()),
    StructField("duration_seconds", LongType()),
    StructField("geo_data", StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("ip_address", StringType()),
    ])),
    StructField("device_profile", StructType([
        StructField("type", StringType()),
        StructField("os", StringType()),
        StructField("browser", StringType()),
    ])),
    StructField("viewed_products", ArrayType(StringType())),
    StructField("page_views", ArrayType(page_view)),
    # a MAP: product_id -> {quantity, price}, instead of one column per product
    StructField("cart_contents", MapType(StringType(), cart_item)),
    StructField("conversion_status", StringType()),
    StructField("referrer", StringType()),
])