from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27018")
db = client["ecommerce"]

REVENUE_STATUSES = ["completed", "shipped", "delivered"]


def top_products(n=10):
    pipeline = [
        # 1. keep only orders that count as revenue
        {"$match": {"status": {"$in": REVENUE_STATUSES}}},

        # 2. one row per line item
        {"$unwind": "$items"},

        # 3. group by product: total units, total revenue, number of orders
        {"$group": {
            "_id": "$items.product_id",
            "units_sold": {"$sum": "$items.quantity"},
            "revenue": {"$sum": "$items.subtotal"},
            "orders": {"$sum": 1},
        }},

        # 4. best sellers first (ties broken by revenue)
        {"$sort": {"units_sold": -1, "revenue": -1}},

        # 5. only the top n
        {"$limit": n},

        # 6. join to the products collection to get name and category
        {"$lookup": {
            "from": "products",
            "localField": "_id",
            "foreignField": "_id",
            "as": "product",
        }},
        {"$unwind": "$product"},

        # 7. choose the output fields
        {"$project": {
            "_id": 0,
            "product_id": "$_id",
            "name": "$product.name",
            "category": "$product.category.name",
            "units_sold": 1,
            "revenue": {"$round": ["$revenue", 2]},
            "orders": 1,
        }},
    ]
    return list(db.transactions.aggregate(pipeline))


def revenue_by_category_week():
    pipeline = [
        # keep only revenue orders, one row per line item
        {"$match": {"status": {"$in": REVENUE_STATUSES}}},
        {"$unwind": "$items"},

        # group by (category, week); $dateTrunc rounds a timestamp down to its week start
        {"$group": {
            "_id": {
                "category_id": "$items.category_id",
                "week": {"$dateTrunc": {"date": "$timestamp", "unit": "week"}},
            },
            "revenue": {"$sum": "$items.subtotal"},
            "units": {"$sum": "$items.quantity"},
        }},

        # oldest week first; within a week, biggest category first
        {"$sort": {"_id.week": 1, "revenue": -1}},

        # flatten the compound _id into plain fields
        {"$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "week": "$_id.week",
            "revenue": {"$round": ["$revenue", 2]},
            "units": 1,
        }},
    ]
    return list(db.transactions.aggregate(pipeline))


# Human-readable names for each $bucket lower boundary
SEGMENT_LABELS = {
    0: "No purchase",
    1: "One-time (1 order)",
    2: "Occasional (2-3 orders)",
    4: "Regular (4-6 orders)",
    7: "Loyal (7+ orders)",
}


def user_segments():
    pipeline = [
        # sort every user into a bucket by their number of revenue orders
        {"$bucket": {
            "groupBy": "$purchase_summary.total_orders",
            "boundaries": [0, 1, 2, 4, 7, 1000],      # buckets: 0 | 1 | 2-3 | 4-6 | 7+
            "output": {
                "users": {"$sum": 1},
                "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                "total_spent": {"$sum": "$purchase_summary.total_spent"},
            },
        }},
    ]
    rows = []
    for r in db.users.aggregate(pipeline):
        rows.append({
            "segment": SEGMENT_LABELS[r["_id"]],      # _id is the bucket's lower boundary
            "users": r["users"],
            "avg_spent": round(r["avg_spent"], 2),
            "total_spent": round(r["total_spent"], 2),
        })
    return rows


if __name__ == "__main__":
    print("TOP 10 PRODUCTS BY UNITS SOLD")
    for row in top_products():
        print(row)

    weekly = revenue_by_category_week()
    print(f"\nWEEKLY REVENUE BY CATEGORY: {len(weekly)} rows, first 8:")
    for row in weekly[:8]:
        print(row)

    print("\nUSER SEGMENTS BY PURCHASE FREQUENCY")
    for row in user_segments():
        print(row)