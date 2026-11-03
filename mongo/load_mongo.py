import json
from datetime import datetime
from pymongo import MongoClient

# 1. Connect to the container's MongoDB (port 27018, see docker-compose.yml)
client = MongoClient("mongodb://localhost:27018")
db = client["ecommerce"]          # the database; Mongo creates it on first write

DATA = "data"                     # folder holding the generated JSON files

# Order statuses that count as real revenue ('processing' does not)
REVENUE_STATUSES = {"completed", "shipped", "delivered"}


def load_json(filename):
    """Read one JSON file and return its contents as Python lists/dicts."""
    with open(f"{DATA}/{filename}", "r", encoding="utf-8") as f:
        return json.load(f)


def to_date(text):
    """Turn '2026-05-16T17:14:02' into a real datetime object (None stays None)."""
    return datetime.fromisoformat(text) if text else None


# 2. Categories ---------------------------------------------------------
db.categories.drop()              # start clean, so re-running never duplicates data

categories = load_json("categories.json")

# Every Mongo document needs a unique _id. We reuse category_id as the _id.
docs = []
for c in categories:
    doc = dict(c)                 # copy the record
    doc["_id"] = doc.pop("category_id")   # rename category_id -> _id
    docs.append(doc)

db.categories.insert_many(docs)
print("categories loaded:", db.categories.count_documents({}))


# 3. Products -----------------------------------------------------------
db.products.drop()

products = load_json("products.json")

# Quick lookup: category_id -> category name
category_name = {c["category_id"]: c["name"] for c in categories}

docs = []
for p in products:
    docs.append({
        "_id": p["product_id"],
        "name": p["name"],
        # embed a small copy of the category (id + name) inside the product
        "category": {
            "category_id": p["category_id"],
            "name": category_name.get(p["category_id"]),
        },
        "base_price": p["base_price"],
        "current_stock": p["current_stock"],
        "is_active": p["is_active"],
        # price_history stays an embedded list, but each date becomes a real Date
        "price_history": [
            {"price": h["price"], "date": to_date(h["date"])} for h in p["price_history"]
        ],
        "creation_date": to_date(p["creation_date"]),
    })

db.products.insert_many(docs)
print("products loaded:", db.products.count_documents({}))


# 4. Transactions (+ per-user purchase summary) -------------------------
db.transactions.drop()

transactions = load_json("transactions.json")

# Quick lookup: product_id -> category_id, so each line item can carry its category
product_category = {p["product_id"]: p["category_id"] for p in products}

# Running totals per user; defaultdict-style, filled in as we loop
summary = {}

docs = []
for t in transactions:
    when = to_date(t["timestamp"])

    # keep the items list embedded, but add category_id to every item
    items = []
    for it in t["items"]:
        item = dict(it)
        item["category_id"] = product_category.get(it["product_id"])
        items.append(item)

    docs.append({
        "_id": t["transaction_id"],
        "session_id": t["session_id"],        # may be None: not every purchase has a session
        "user_id": t["user_id"],
        "timestamp": when,
        "items": items,
        "subtotal": t["subtotal"],
        "discount": t["discount"],
        "total": t["total"],
        "payment_method": t["payment_method"],
        "status": t["status"],
    })

    # update this user's summary, but only for orders that count as revenue
    if t["status"] in REVENUE_STATUSES:
        s = summary.setdefault(t["user_id"], {
            "total_orders": 0, "total_spent": 0.0,
            "first_purchase": when, "last_purchase": when,
        })
        s["total_orders"] += 1
        s["total_spent"] = round(s["total_spent"] + t["total"], 2)
        s["first_purchase"] = min(s["first_purchase"], when)
        s["last_purchase"] = max(s["last_purchase"], when)

db.transactions.insert_many(docs)
print("transactions loaded:", db.transactions.count_documents({}))


# 5. Users (profile + embedded purchase summary) ------------------------
db.users.drop()

users = load_json("users.json")

# Users who never bought get an all-zero summary
empty_summary = {"total_orders": 0, "total_spent": 0.0,
                 "first_purchase": None, "last_purchase": None}

docs = []
for u in users:
    docs.append({
        "_id": u["user_id"],
        "geo_data": u["geo_data"],
        "registration_date": to_date(u["registration_date"]),
        "last_active": to_date(u["last_active"]),
        "purchase_summary": summary.get(u["user_id"], empty_summary),
    })

db.users.insert_many(docs)
print("users loaded:", db.users.count_documents({}))

# 6. Sessions (subset: first chunk file = 100,000 sessions) --------------
db.sessions.drop()

sessions = load_json("sessions_0.json")

batch = []
for s in sessions:
    s["_id"] = s.pop("session_id")                 # session_id becomes the _id
    s["start_time"] = to_date(s["start_time"])
    s["end_time"] = to_date(s["end_time"])
    for pv in s["page_views"]:                     # dates inside the nested list too
        pv["timestamp"] = to_date(pv["timestamp"])
    batch.append(s)

    if len(batch) == 5000:                         # insert in batches to save memory
        db.sessions.insert_many(batch)
        batch = []

if batch:                                          # insert the last partial batch
    db.sessions.insert_many(batch)

print("sessions loaded:", db.sessions.count_documents({}))


# 7. Indexes (chosen to match the queries we will run) ------------------
db.transactions.create_index([("user_id", 1), ("timestamp", -1)])  # a user's orders, newest first
db.transactions.create_index([("timestamp", 1)])                   # revenue over time
db.transactions.create_index([("items.product_id", 1)])            # sales per product
db.products.create_index([("category.category_id", 1)])            # products in a category
db.sessions.create_index([("user_id", 1), ("start_time", -1)])     # a user's sessions, newest first
db.sessions.create_index([("viewed_products", 1)])                 # sessions that viewed a product
print("indexes created")