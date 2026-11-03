from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27018")
db = client["ecommerce"]

pipeline = [
    # revenue orders only, same rule as before
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},

    # one group per week (all categories together this time)
    {"$group": {
        "_id": {"$dateTrunc": {"date": "$timestamp", "unit": "week"}},
        "orders": {"$sum": 1},
        "revenue": {"$sum": "$total"},
        "first_order": {"$min": "$timestamp"},   # earliest order in the week
        "last_order": {"$max": "$timestamp"},    # latest order in the week
    }},
    {"$sort": {"_id": 1}},
]

for row in db.transactions.aggregate(pipeline):
    print(row["_id"].date(), "| orders:", row["orders"],
          "| revenue:", round(row["revenue"]),
          "| first:", row["first_order"].date(), "| last:", row["last_order"].date())