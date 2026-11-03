import json

DATA = "data"

def load(name):
    # json.load reads a whole JSON file into Python lists/dicts
    with open(f"{DATA}/{name}", "r", encoding="utf-8") as f:
        return json.load(f)

# The small files first
for name in ["categories.json", "products.json", "users.json", "transactions.json"]:
    rows = load(name)
    print(f"\n===== {name}: {len(rows)} records =====")
    # indent=2 pretty-prints, so you can see the nesting
    print(json.dumps(rows[0], indent=2))

# One sessions file (about 190 MB, so this takes a few seconds)
sessions = load("sessions_0.json")
print(f"\n===== sessions_0.json: {len(sessions)} records =====")
print(json.dumps(sessions[0], indent=2))