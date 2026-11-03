import json
from datetime import datetime, timezone
import happybase

# 1. Connect through the Thrift server (port 9090, mapped in docker-compose.yml)
connection = happybase.Connection("localhost", port=9090)
print("Tables in HBase:", connection.tables())

table = connection.table("user_sessions")

MAX_TS = 9999999999999            # 13 digits, bigger than any timestamp in ms


def make_row_key(user_id, start_iso):
    """user_id#reverse_timestamp, so a user's NEWEST session sorts first."""
    dt = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
    millis = int(dt.timestamp() * 1000)
    reverse = MAX_TS - millis
    return f"{user_id}#{reverse:013d}"


# 2. One session, copied from what we saw in MongoDB
user_id = "user_007129"
start = "2026-09-26T05:12:49"
row_key = make_row_key(user_id, start)
print("Row key:", row_key)

# 3. HBase stores everything as bytes, so every key and value is .encode()d.
#    Column names look like  family:qualifier
table.put(row_key.encode(), {
    b"meta:session_id": b"sess_9e84ae419c",
    b"meta:start_time": start.encode(),
    b"meta:duration_seconds": b"3216",
    b"meta:referrer": b"direct",
    b"meta:conversion_status": b"converted",
    b"device:type": b"mobile",
    b"device:os": b"Windows",
    b"device:browser": b"Firefox",
    # page views: one column per view, qualifier = its position (0001, 0002, ...)
    b"pv:0001": json.dumps({"page_type": "home", "product_id": None, "duration": 111}).encode(),
    b"pv:0002": json.dumps({"page_type": "product_detail", "product_id": "prod_04873", "duration": 94}).encode(),
})

# 4. Read it back by its exact row key
print("\nGET by row key:")
for column, value in table.row(row_key.encode()).items():
    print(" ", column.decode(), "=", value.decode())

# 5. Scan every row whose key starts with this user's id
print("\nSCAN for user_007129:")
for key, data in table.scan(row_prefix=b"user_007129#"):
    print(" ", key.decode(), "->", data[b"meta:session_id"].decode())