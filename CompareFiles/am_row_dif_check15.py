import pandas as pd

am_ground = pd.read_csv(
    "C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv",
    low_memory=False,
)

subset = am_ground[(am_ground["id"] == 15) & (am_ground["do_session"] == "DO2")].copy()
print("id=15 DO2 rows:", subset.shape[0])

subset["date_time"] = pd.to_datetime(subset["date_time"], errors="coerce")
print("date_time nulls:", subset["date_time"].isna().sum())

print("date_time min/max:", subset["date_time"].min(), subset["date_time"].max())
print("unique dates:", subset["date_time"].dt.date.nunique())
print("dates value counts (top 10):")
print(subset["date_time"].dt.date.value_counts().head(10))

# time-only analysis
subset["time_only"] = subset["date_time"].dt.time
print("unique time_only count:", subset["time_only"].nunique())
print("duplicates in time_only:", subset.shape[0] - subset["time_only"].nunique())

# per-date duration
per_date = (
    subset.dropna(subset=["date_time"])
    .groupby(subset["date_time"].dt.date)["date_time"]
    .agg(first="min", last="max", rows="size", uniq_times=lambda s: s.dt.time.nunique())
    .reset_index()
)
per_date["duration_seconds"] = (per_date["last"] - per_date["first"]).dt.total_seconds().astype("Int64")
per_date["duration_hms"] = pd.to_timedelta(per_date["duration_seconds"], unit="s").astype(str)
print("per-date summary (top 10):")
print(per_date.sort_values("rows", ascending=False).head(10))
