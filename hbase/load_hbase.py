import argparse
import json
import time
from datetime import datetime, timezone
import happybase

# --limit lets us test on a small slice before loading everything
parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=None, help="max sessions to load")
args = parser.parse_args()

DATA = "data"
MAX_TS = 9999999999999            # 13 digits, bigger than any timestamp in ms

connection = happybase.Connection("localhost", port=9090)


def make_row_key(user_id, start_iso, session_id):
    """user_id#reverse_timestamp#session_id : a user's NEWEST session sorts first."""
    dt = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
    millis = int(dt.timestamp() * 1000)
    return f"{user_id}#{MAX_TS - millis:013d}#{session_id}"


# 1. (Re)create the table: three column families -----------------------
if b"user_sessions" in connection.tables():
    connection.disable_table("user_sessions")     # a table must be disabled before deleting
    connection.delete_table("user_sessions")
connection.create_table("user_sessions", {"meta": dict(), "device": dict(), "pv": dict()})
table = connection.table("user_sessions")
print("table user_sessions created")

# 2. Read the sessions file ---------------------------------------------
with open(f"{DATA}/sessions_0.json", "r", encoding="utf-8") as f:
    sessions = json.load(f)
if args.limit:
    sessions = sessions[: args.limit]
print(f"loading {len(sessions):,} sessions...")

# 3. Write in batches of 1000 rows --------------------------------------
start_time = time.time()
with table.batch(batch_size=1000) as batch:
    for i, s in enumerate(sessions, start=1):
        row = {
            b"meta:session_id": s["session_id"].encode(),
            b"meta:start_time": s["start_time"].encode(),
            b"meta:duration_seconds": str(s["duration_seconds"]).encode(),
            b"meta:referrer": s["referrer"].encode(),
            b"meta:conversion_status": s["conversion_status"].encode(),
            b"meta:viewed_products": json.dumps(s["viewed_products"]).encode(),
            b"meta:cart_contents": json.dumps(s["cart_contents"]).encode(),
            b"device:type": s["device_profile"]["type"].encode(),
            b"device:os": s["device_profile"]["os"].encode(),
            b"device:browser": s["device_profile"]["browser"].encode(),
        }
        # one column per page view, qualifier = its position (0001, 0002, ...)
        for n, pv in enumerate(s["page_views"], start=1):
            row[f"pv:{n:04d}".encode()] = json.dumps({
                "ts": pv["timestamp"],
                "page_type": pv["page_type"],
                "product_id": pv["product_id"],
                "category_id": pv["category_id"],
                "duration": pv["view_duration"],
            }).encode()

        key = make_row_key(s["user_id"], s["start_time"], s["session_id"])
        batch.put(key.encode(), row)

        if i % 5000 == 0:
            print(f"  {i:,} sessions written ({time.time() - start_time:.0f}s)")

print(f"done in {time.time() - start_time:.0f} seconds")