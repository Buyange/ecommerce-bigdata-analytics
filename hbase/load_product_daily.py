import json
import time
from collections import defaultdict
import happybase

DATA = "data"
connection = happybase.Connection("localhost", port=9090)

# 1. (Re)create the table with one column family 'm' (metrics) ---------
if b"product_daily" in connection.tables():
    connection.disable_table("product_daily")
    connection.delete_table("product_daily")
connection.create_table("product_daily", {"m": dict()})
table = connection.table("product_daily")
print("table product_daily created")

# 2. Read the sessions and count per (product, day) --------------------
with open(f"{DATA}/sessions_0.json", "r", encoding="utf-8") as f:
    sessions = json.load(f)

stats = defaultdict(lambda: {"views": 0, "cart_adds": 0, "purchases": 0})

for s in sessions:
    day = s["start_time"][:10].replace("-", "")        # '2026-09-26T05:12:49' -> '20260926'

    # a product-detail page view counts as one view of that product
    for pv in s["page_views"]:
        if pv["page_type"] == "product_detail" and pv["product_id"]:
            stats[(pv["product_id"], day)]["views"] += 1

    # every product in the cart counts one cart add; if the session
    # converted, its quantity also counts as purchased units
    for product_id, item in (s["cart_contents"] or {}).items():
        stats[(product_id, day)]["cart_adds"] += 1
        if s["conversion_status"] == "converted":
            stats[(product_id, day)]["purchases"] += item["quantity"]

print(f"{len(stats):,} product-day rows to write")

# 3. Write each row once, in batches -----------------------------------
start = time.time()
with table.batch(batch_size=1000) as batch:
    for (product_id, day), m in stats.items():
        key = f"{product_id}#{day}"
        batch.put(key.encode(), {
            b"m:views": str(m["views"]).encode(),
            b"m:cart_adds": str(m["cart_adds"]).encode(),
            b"m:purchases": str(m["purchases"]).encode(),
        })
print(f"written in {time.time() - start:.0f} seconds")

# 4. Demo queries on the most-viewed product ---------------------------
total_views = defaultdict(int)
for (product_id, day), m in stats.items():
    total_views[product_id] += m["views"]
top = max(total_views, key=total_views.get)

print(f"\nMost-viewed product: {top} ({total_views[top]} views)")

print("\nAll days (prefix scan):")
for key, data in table.scan(row_prefix=f"{top}#".encode()):
    print(" ", key.decode().split("#")[1],
          "views", data[b"m:views"].decode(),
          "| cart adds", data[b"m:cart_adds"].decode(),
          "| purchases", data[b"m:purchases"].decode())

print("\nOnly 1-7 October (range scan, start inclusive, stop exclusive):")
for key, data in table.scan(row_start=f"{top}#20261001".encode(),
                            row_stop=f"{top}#20261008".encode()):
    print(" ", key.decode().split("#")[1],
          "views", data[b"m:views"].decode(),
          "| cart adds", data[b"m:cart_adds"].decode(),
          "| purchases", data[b"m:purchases"].decode())