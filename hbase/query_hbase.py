import argparse
from datetime import datetime, timezone
import happybase

parser = argparse.ArgumentParser()
parser.add_argument("--user", default="user_007129")
parser.add_argument("--from_date", default="2026-09-01T00:00:00")
parser.add_argument("--to_date", default="2026-09-30T23:59:59")
args = parser.parse_args()

MAX_TS = 9999999999999

connection = happybase.Connection("localhost", port=9090)
table = connection.table("user_sessions")


def to_millis(iso):
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp() * 1000)


def show(key, data):
    """Print one session row in a readable form."""
    n_views = sum(1 for col in data if col.startswith(b"pv:"))   # count the pv:* columns
    print(f"  {data[b'meta:start_time'].decode()} | {data[b'meta:session_id'].decode()} | "
          f"{data[b'meta:conversion_status'].decode():9} | {data[b'device:type'].decode():7} | "
          f"{n_views} page views")


# Query 1: the user's 5 most recent sessions ---------------------------
# The reverse timestamp means rows for this user are already newest-first.
print(f"\nQ1. Latest 5 sessions of {args.user}")
for key, data in table.scan(row_prefix=f"{args.user}#".encode(), limit=5):
    show(key, data)

# Query 2: the user's sessions between two dates -----------------------
# Later time = SMALLER reverse number, so the range runs from the END date
# (row_start, inclusive) to just after the START date (row_stop, exclusive).
row_start = f"{args.user}#{MAX_TS - to_millis(args.to_date):013d}"
row_stop = f"{args.user}#{MAX_TS - to_millis(args.from_date) + 1:013d}"
print(f"\nQ2. Sessions of {args.user} from {args.from_date} to {args.to_date}")
found = 0
for key, data in table.scan(row_start=row_start.encode(), row_stop=row_stop.encode()):
    show(key, data)
    found += 1
print(f"  -> {found} session(s)")

# Query 3: how many rows are in the table? -----------------------------
# KeyOnlyFilter makes HBase send only the keys, so the scan is much lighter.
total = sum(1 for _ in table.scan(filter=b"KeyOnlyFilter()"))
print(f"\nQ3. Total rows in user_sessions: {total:,}")