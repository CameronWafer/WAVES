import pandas as pd

behav_am_df = pd.read_excel(
    "C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx",
    engine="openpyxl",
)

# Keep only State start rows as in pipeline
behav_am_df = behav_am_df[behav_am_df["Event_Type"] == "State start"].copy()

# Ensure datetime
behav_am_df["Date_Time_Absolute_dmy_hmsf"] = pd.to_datetime(
    behav_am_df["Date_Time_Absolute_dmy_hmsf"], errors="coerce"
)

# Extract id and do from Observation
behav_am_df["id"] = (
    behav_am_df["Observation"].str.extract(r"AM(\d{2})", expand=False).astype("int64")
)
behav_am_df["do"] = (
    behav_am_df["Observation"]
    .str.extract(r"(DO\d+(?:_[ab])?)", expand=False)
    .astype("string")
    .str.strip()
)

target = behav_am_df[(behav_am_df["id"] == 15) & (behav_am_df["do"].str.startswith("DO2"))].copy()

print("Rows for id=15 DO2*:", target.shape[0])
print("Unique Observations:", target["Observation"].nunique())
print("Observations list:")
print(target["Observation"].value_counts())

# Per-Observation time range
per_obs = (
    target.groupby("Observation")["Date_Time_Absolute_dmy_hmsf"]
    .agg(first="min", last="max", rows="size")
    .reset_index()
)
per_obs["duration_seconds"] = (
    per_obs["last"] - per_obs["first"]
).dt.total_seconds()
per_obs["duration_seconds"] = pd.to_numeric(per_obs["duration_seconds"], errors="coerce")

print("\nPer-Observation time ranges:")
print(per_obs.sort_values("first"))

# Dates represented per Observation
per_obs_dates = (
    target.assign(date=target["Date_Time_Absolute_dmy_hmsf"].dt.date)
    .groupby("Observation")["date"]
    .agg(unique_dates="nunique")
    .reset_index()
)
print("\nUnique dates per Observation:")
print(per_obs_dates)

# Per-date summary for id 15 DO2
per_date = (
    target.assign(date=target["Date_Time_Absolute_dmy_hmsf"].dt.date)
    .groupby("date")["Date_Time_Absolute_dmy_hmsf"]
    .agg(first="min", last="max", rows="size")
    .reset_index()
)
per_date["duration_seconds"] = (
    per_date["last"] - per_date["first"]
).dt.total_seconds()
per_date["duration_seconds"] = pd.to_numeric(per_date["duration_seconds"], errors="coerce")
print("\nPer-date time ranges:")
print(per_date.sort_values("first"))

# Check overlaps between observations (by time ranges)
per_obs_sorted = per_obs.sort_values("first").reset_index(drop=True)
print("\nOverlap checks:")
for i in range(len(per_obs_sorted) - 1):
    a = per_obs_sorted.loc[i]
    b = per_obs_sorted.loc[i + 1]
    overlap = a["last"] >= b["first"]
    if overlap:
        print(
            f"Overlap between {a['Observation']} and {b['Observation']}:",
            a["last"],
            ">=",
            b["first"],
        )
