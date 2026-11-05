import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

os.makedirs("viz/figures", exist_ok=True)
db = MongoClient("mongodb://localhost:27018")["ecommerce"]

LABELS = {0: "No purchase", 1: "One-time\n(1 order)", 2: "Occasional\n(2-3 orders)",
          4: "Regular\n(4-6 orders)", 7: "Loyal\n(7+ orders)"}

# same $bucket idea as in mongo/aggregations.py, using the embedded purchase_summary
pipeline = [{"$bucket": {
    "groupBy": "$purchase_summary.total_orders",
    "boundaries": [0, 1, 2, 4, 7, 1000],
    "output": {"users": {"$sum": 1}, "revenue": {"$sum": "$purchase_summary.total_spent"}},
}}]
df = pd.DataFrame(list(db.users.aggregate(pipeline))).sort_values("_id")
df["segment"] = df["_id"].map(LABELS)
df["pct_users"] = 100 * df["users"] / df["users"].sum()
df["pct_revenue"] = 100 * df["revenue"] / df["revenue"].sum()

x = np.arange(len(df))
w = 0.38
fig, ax = plt.subplots(figsize=(10, 5))
b1 = ax.bar(x - w / 2, df["pct_users"], w, label="Share of users", color="#9ecae1")
b2 = ax.bar(x + w / 2, df["pct_revenue"], w, label="Share of revenue", color="#2f6f9f")
for bars in (b1, b2):
    for bar in bars:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.8,
                f"{bar.get_height():.1f}%", ha="center", fontsize=9)

ax.set_xticks(x)
ax.set_xticklabels(df["segment"])
ax.set_ylabel("Percent of total")
ax.set_title("Customer segments by purchase frequency: share of users vs share of revenue")
ax.legend()
ax.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
plt.savefig("viz/figures/user_segments.png", dpi=150)
print("saved viz/figures/user_segments.png")