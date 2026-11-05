import os
import pandas as pd
import matplotlib.pyplot as plt

os.makedirs("viz/figures", exist_ok=True)

df = pd.read_csv("results/funnel.csv")
df = df.iloc[::-1]                       # reverse, so the widest bar ends up on top

fig, ax = plt.subplots(figsize=(10, 4.8))
bars = ax.barh(df["stage"], df["sessions"], color="#2f6f9f")

# label every bar with its count, share of all sessions and share of the previous stage
for bar, (_, row) in zip(bars, df.iterrows()):
    label = f'{row["sessions"]:,}  ({row["pct_of_all"]:.1f}% of all'
    if row["pct_of_previous"] < 100:
        label += f', {row["pct_of_previous"]:.1f}% of previous stage'
    label += ")"
    ax.text(bar.get_width() + 1500, bar.get_y() + bar.get_height() / 2,
            label, va="center", fontsize=9)

ax.set_xlim(0, df["sessions"].max() * 1.65)
ax.set_xlabel("Sessions")
ax.set_title("Conversion funnel: from first visit to purchase\n"
             "(100,000 sessions; stages from HBase, orders from MongoDB)")
ax.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
plt.savefig("viz/figures/funnel.png", dpi=150)
print("saved viz/figures/funnel.png")