import os
import pandas as pd
import matplotlib.pyplot as plt

os.makedirs("viz/figures", exist_ok=True)

df = pd.read_csv("results/cohort.csv")

# Full months only; days of data in each (August starts on the 4th)
days = {"2026-08": 28, "2026-09": 30, "2026-10": 31}
df = df[df["purchase_month"].isin(days)].copy()
df["rev_per_user_per_day"] = df["revenue_per_user"] / df["purchase_month"].map(days)

# Rows = registration cohort, columns = purchase month
pivot = df.pivot(index="registration_month", columns="purchase_month",
                 values="rev_per_user_per_day")

fig, ax = plt.subplots(figsize=(7.5, 5))
vmax = pivot.values.max() * 1.15
im = ax.imshow(pivot.values, cmap="Blues", vmin=0, vmax=vmax, aspect="auto")   # scale starts at 0

ax.set_xticks(range(len(pivot.columns)))
ax.set_xticklabels(pivot.columns)
ax.set_yticks(range(len(pivot.index)))
ax.set_yticklabels(pivot.index)
ax.set_xlabel("Purchase month")
ax.set_ylabel("Registration cohort")

# write the value inside each cell
for i in range(pivot.shape[0]):
    for j in range(pivot.shape[1]):
        v = pivot.values[i, j]
        ax.text(j, i, f"{v:.1f}", ha="center", va="center",
                color="white" if v > vmax * 0.55 else "black", fontsize=10)

fig.colorbar(im, ax=ax, label="Revenue per user per day")
ax.set_title("Cohort analysis: spending does not depend on registration month\n"
             "(August cohort is small: 195 users)")

plt.tight_layout()
plt.savefig("viz/figures/cohort_heatmap.png", dpi=150)
print("saved viz/figures/cohort_heatmap.png")