import os
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

os.makedirs("viz/figures", exist_ok=True)
db = MongoClient("mongodb://localhost:27018")["ecommerce"]

pipeline = [
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},   # revenue orders only
    {"$group": {"_id": {"$dateTrunc": {"date": "$timestamp", "unit": "week"}},
                "revenue": {"$sum": "$total"},
                "orders": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
]
df = pd.DataFrame(list(db.transactions.aggregate(pipeline))).rename(columns={"_id": "week"})

# the first and last weeks only cover part of a week, so leave them out
print("dropping partial weeks starting", df["week"].iloc[0].date(), "and", df["week"].iloc[-1].date())
df = df.iloc[1:-1]

mean = df["revenue"].mean()
labels = df["week"].dt.strftime("%d %b")

fig, ax = plt.subplots(figsize=(10, 4.8))
ax.bar(labels, df["revenue"] / 1e6, color="#2f6f9f")
ax.axhline(mean / 1e6, color="gray", linestyle="--", label=f"average {mean / 1e6:.2f}M")
ax.set_ylim(0, df["revenue"].max() / 1e6 * 1.25)               # axis starts at 0
ax.set_ylabel("Revenue (millions)")
ax.set_xlabel("Week starting (Sunday)")
ax.set_title(f"Weekly revenue is stable at about {mean / 1e6:.1f}M ({len(df)} full weeks)")
ax.legend(loc="upper right")
ax.spines[["top", "right"]].set_visible(False)
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig("viz/figures/weekly_revenue.png", dpi=150)
print("saved viz/figures/weekly_revenue.png")