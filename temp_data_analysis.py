"""
Temporary data analysis: am_behposture_onesheet.xlsx
Focus: structure, time_relative_hms, time_absolute_hms by Observation.
"""
import pandas as pd
import numpy as np

path = r"C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx"
print("=" * 60)
print("1. LOAD RAW DATA (all rows)")
print("=" * 60)
df = pd.read_excel(path, engine="openpyxl")
print(f"Shape: {df.shape}")
print(f"\nColumns:\n{list(df.columns)}")
print(f"\nDtypes:\n{df.dtypes}")

print("\n" + "=" * 60)
print("2. UNIQUE OBSERVATION VALUES")
print("=" * 60)
obs = df["Observation"].dropna().astype(str).unique()
print(f"Count: {len(obs)}")
for o in sorted(obs):
    n = (df["Observation"].astype(str) == o).sum()
    print(f"  {o}: {n} rows")

print("\n" + "=" * 60)
print("3. TIME COLUMNS (names containing time/relative/absolute)")
print("=" * 60)
time_cols = [c for c in df.columns if any(
    x in c.lower() for x in ["time", "relative", "absolute", "hms", "hmsf"]
)]
print(time_cols)
for c in time_cols:
    print(f"\n  {c}: dtype={df[c].dtype}, sample={df[c].iloc[0]}, nulls={df[c].isna().sum()}")

# Prefer exact names if present
rel_col = "Time_Relative_hms" if "Time_Relative_hms" in df.columns else None
abs_col = "Time_Absolute_hms" if "Time_Absolute_hms" in df.columns else None
if rel_col is None:
    rel_col = [c for c in df.columns if "relative" in c.lower() and "hms" in c.lower()]
    rel_col = rel_col[0] if rel_col else None
if abs_col is None:
    abs_col = [c for c in df.columns if "absolute" in c.lower() and "hms" in c.lower()]
    abs_col = abs_col[0] if abs_col else None
print(f"\nUsing relative time col: {rel_col}, absolute time col: {abs_col}")

print("\n" + "=" * 60)
print("4. TIME RANGES BY OBSERVATION (relative and absolute)")
print("=" * 60)
for ob in sorted(df["Observation"].dropna().astype(str).unique()):
    sub = df[df["Observation"].astype(str) == ob]
    rel = sub[rel_col] if rel_col else None
    abs_ = sub[abs_col] if abs_col else None
    rel_min, rel_max = (rel.min(), rel.max()) if rel is not None and rel.notna().any() else (None, None)
    abs_min, abs_max = (abs_.min(), abs_.max()) if abs_ is not None and abs_.notna().any() else (None, None)
    print(f"\n  {ob}:")
    print(f"    rows: {len(sub)}")
    if rel_col:
        print(f"    {rel_col}: min={rel_min}, max={rel_max}")
    if abs_col:
        print(f"    {abs_col}: min={abs_min}, max={abs_max}")

print("\n" + "=" * 60)
print("5. EVENT_TYPE (if present)")
print("=" * 60)
if "Event_Type" in df.columns:
    print(df["Event_Type"].value_counts(dropna=False))
else:
    print("No Event_Type column")

print("\n" + "=" * 60)
print("6. STATE START ONLY (as in notebook) - time by Observation")
print("=" * 60)
if "Event_Type" in df.columns:
    state = df[df["Event_Type"] == "State start"].copy()
    print(f"Rows after State start filter: {len(state)}")
    for col in state.columns:
        if pd.api.types.is_timedelta64_dtype(state[col]):
            base = pd.Timestamp("1900-01-01")
            state[col] = (base + state[col]).dt.strftime("%H:%M:%S")
    for ob in sorted(state["Observation"].dropna().astype(str).unique()):
        sub = state[state["Observation"].astype(str) == ob]
        rel = sub[rel_col] if rel_col else None
        abs_ = sub[abs_col] if abs_col else None
        rel_min, rel_max = (rel.min(), rel.max()) if rel is not None and rel.notna().any() else (None, None)
        abs_min, abs_max = (abs_.min(), abs_.max()) if abs_ is not None and abs_.notna().any() else (None, None)
        print(f"\n  {ob}: n={len(sub)}, {rel_col}={rel_min}..{rel_max}, {abs_col}={abs_min}..{abs_max}")
else:
    print("Skipped (no Event_Type)")

print("\n" + "=" * 60)
print("7. HEAD OF RAW DATA (first 3 rows)")
print("=" * 60)
print(df.head(3).to_string())

print("\n" + "=" * 60)
print("8. OBSERVATIONS WITH NEGATIVE Time_Relative_hms (copyB sessions)")
print("=" * 60)
# In raw data, some Observations (copyB) share session start with copyA so relative time can go negative
df["_rel_sec"] = pd.to_timedelta(df["Time_Relative_hms"].astype(str)).dt.total_seconds()
neg = df.groupby("Observation")["_rel_sec"].min()
neg = neg[neg < 0]
for ob in neg.index:
    print(f"  {ob}: min Time_Relative_hms = {neg[ob]/3600:.2f} hours (same session start as copyA)")

print("\n" + "=" * 60)
print("9. STRUCTURE SUMMARY")
print("=" * 60)
print("""
- Rows: event-based (State start / State stop / State point). Each row = one behavior state START with duration.
- Observation: session ID (AMnnDOk or AMnnDOk_copyA/copyB). id = AM number, do = DO1/DO2(_a/_b).
- Time_Relative_hms: time since session start (per Observation). Can be negative for copyB (session start = copyA start).
- Time_Absolute_hms: wall-clock time (datetime).
- Notebook: keeps State start only, builds SECOND-BY-SECOND grid per Observation using Date_Time_Absolute_dmy_hmsf
  and Duration_sf; assigns the latest-starting event covering each second; then maps Behavior -> Activity/Posture
  and exports Cameron_AM_Clean.csv.
""")
print("Done.")
