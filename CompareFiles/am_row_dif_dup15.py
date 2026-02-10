import pandas as pd

am_ground = pd.read_csv(
    "C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv",
    low_memory=False,
)

subset = am_ground[(am_ground["id"] == 15) & (am_ground["do_session"] == "DO2")].copy()
subset["date_time"] = pd.to_datetime(subset["date_time"], errors="coerce")

first_date = subset["date_time"].dt.date.min()
filtered = subset[subset["date_time"].dt.date == first_date].copy()
filtered = filtered.sort_values("date_time")

print("Filtered rows:", filtered.shape[0])
print("Unique seconds:", filtered["date_time"].nunique())

# Find duplicate seconds
dup_mask = filtered.duplicated(subset=["date_time"], keep=False)
dups = filtered[dup_mask].copy()
print("Duplicate rows:", dups.shape[0])
print("Duplicate seconds:", dups["date_time"].nunique())

# Check if duplicates are identical across all non-time columns
cols_to_check = [c for c in filtered.columns if c not in ["date_time", "time"]]

conflicts = (
    dups.groupby("date_time")[cols_to_check]
    .nunique(dropna=False)
    .reset_index()
)
conflict_cols = [c for c in cols_to_check if conflicts[c].max() > 1]

print("Columns with conflicting values among duplicates:", conflict_cols)
if conflict_cols:
    print("Example conflicting seconds (top 5):")
    example_times = conflicts[conflicts[conflict_cols].max(axis=1) > 1]["date_time"].head(5)
    for t in example_times:
        print("\nTime:", t)
        print(dups[dups["date_time"] == t][["date_time"] + conflict_cols].head(10))
else:
    print("Duplicates appear identical across checked columns.")

# If conflicts exist, show how often they happen
if conflict_cols:
    conflict_counts = (conflicts[conflict_cols] > 1).sum().sort_values(ascending=False)
    print("\nConflict counts by column:")
    print(conflict_counts)
import pandas as pd

am_ground = pd.read_csv(
    "C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv",
    low_memory=False,
)

subset = am_ground[(am_ground["id"] == 15) & (am_ground["do_session"] == "DO2")].copy()
subset["date_time"] = pd.to_datetime(subset["date_time"], errors="coerce")

first_date = subset["date_time"].dt.date.min()
filtered = subset[subset["date_time"].dt.date == first_date].copy()
filtered = filtered.sort_values("date_time")

print("Filtered rows:", filtered.shape[0])
print("Unique seconds:", filtered["date_time"].nunique())

# Find duplicate seconds
dup_mask = filtered.duplicated(subset=["date_time"], keep=False)
dups = filtered[dup_mask].copy()
print("Duplicate rows:", dups.shape[0])
print("Duplicate seconds:", dups["date_time"].nunique())

# Check if duplicates are identical across all non-time columns
cols_to_check = [c for c in filtered.columns if c not in ["date_time", "time"]]

conflicts = (
    dups.groupby("date_time")[cols_to_check]
    .nunique(dropna=False)
    .reset_index()
)
conflict_cols = [c for c in cols_to_check if conflicts[c].max() > 1]

print("Columns with conflicting values among duplicates:", conflict_cols)
if conflict_cols:
    print("Example conflicting seconds (top 5):")
    example_times = conflicts[conflicts[conflict_cols].max(axis=1) > 1]["date_time"].head(5)
    for t in example_times:
        print("\nTime:", t)
        print(dups[dups["date_time"] == t][["date_time"] + conflict_cols].head(10))
else:
    print("Duplicates appear identical across checked columns.")

# If conflicts exist, show how often they happen
if conflict_cols:
    conflict_counts = (conflicts[conflict_cols] > 1).sum().sort_values(ascending=False)
    print("\nConflict counts by column:")
    print(conflict_counts)
