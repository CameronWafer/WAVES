import pandas as pd

am_ground = pd.read_csv(
    "C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv",
    low_memory=False,
)

subset = am_ground[(am_ground["id"] == 15) & (am_ground["do_session"] == "DO2")].copy()
subset["date_time"] = pd.to_datetime(subset["date_time"], errors="coerce")

print("Original id=15 DO2 rows:", subset.shape[0])
print("Original date range:", subset["date_time"].min(), "->", subset["date_time"].max())

first_date = subset["date_time"].dt.date.min()
filtered = subset[subset["date_time"].dt.date == first_date].copy()

print("\nKeeping only first date:", first_date)
print("Filtered rows:", filtered.shape[0])
print("Filtered date range:", filtered["date_time"].min(), "->", filtered["date_time"].max())

duration_seconds = (filtered["date_time"].max() - filtered["date_time"].min()).total_seconds()
duration_hms = pd.to_timedelta(duration_seconds, unit="s")
print("Filtered duration:", duration_hms)

# duplicates/gaps within the filtered date
filtered = filtered.sort_values("date_time")
unique_times = filtered["date_time"].nunique()
expected = int(duration_seconds) + 1
duplicate_rows = filtered.shape[0] - unique_times
gap_seconds = expected - unique_times

print("Unique seconds:", unique_times)
print("Expected seconds:", expected)
print("Duplicate rows:", duplicate_rows)
print("Gap seconds:", gap_seconds)

# show biggest time gaps if any
diffs = filtered["date_time"].diff().dt.total_seconds()
big_gaps = diffs[diffs > 1]
print("Gap count (>1s):", big_gaps.shape[0])
if not big_gaps.empty:
    print("Top 5 gaps (seconds):")
    print(big_gaps.sort_values(ascending=False).head(5))
