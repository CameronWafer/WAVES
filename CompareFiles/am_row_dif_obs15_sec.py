import pandas as pd
import numpy as np

behav_am_df = pd.read_excel(
    "C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx",
    engine="openpyxl",
)

# Keep only State start rows (as in pipeline)
behav_am_df = behav_am_df[behav_am_df["Event_Type"] == "State start"].copy()

behav_am_df["Date_Time_Absolute_dmy_hmsf"] = pd.to_datetime(
    behav_am_df["Date_Time_Absolute_dmy_hmsf"], errors="coerce"
)

behav_am_df["id"] = (
    behav_am_df["Observation"].str.extract(r"AM(\d{2})", expand=False).astype("int64")
)
behav_am_df["do"] = (
    behav_am_df["Observation"]
    .str.extract(r"(DO\d+(?:_[ab])?)", expand=False)
    .astype("string")
    .str.strip()
)

# Focus on id=15 DO2* only
df = behav_am_df[(behav_am_df["id"] == 15) & (behav_am_df["do"].str.startswith("DO2"))].copy()
df = df.dropna(subset=["Observation", "Date_Time_Absolute_dmy_hmsf"]).copy()

# Floor to seconds for alignment
df["_start_dt_sec"] = df["Date_Time_Absolute_dmy_hmsf"].dt.floor("s")
df["_dur_s"] = pd.to_numeric(df["Duration_sf"], errors="coerce").fillna(0.0)
df["_dur_s_int"] = np.ceil(df["_dur_s"]).astype("int64")
df["_end_dt_sec"] = df["_start_dt_sec"] + pd.to_timedelta(df["_dur_s_int"], unit="s")

# Sort for stable "latest start wins"
df = df.sort_values(["Observation", "_start_dt_sec", "Date_Time_Absolute_dmy_hmsf"], kind="mergesort")

helper_cols = {"_start_dt_sec", "_dur_s", "_dur_s_int", "_end_dt_sec"}
carry_cols = [c for c in df.columns if c not in helper_cols]

out = []
for obs, g in df.groupby("Observation", sort=False):
    g = g.copy()
    start_dt = g["_start_dt_sec"].min()
    end_dt = g["_end_dt_sec"].max()
    grid = pd.date_range(start=start_dt, end=end_dt, freq="1s")

    starts = g["_start_dt_sec"].to_numpy()
    ends = g["_end_dt_sec"].to_numpy()
    tvals = grid.to_numpy()
    idx = np.searchsorted(starts, tvals, side="right") - 1

    res = pd.DataFrame({"Observation": obs, "date_time_abs": grid})
    valid = idx >= 0
    valid &= tvals <= ends[np.maximum(idx, 0)]

    if valid.any():
        take_rows = g.iloc[idx[valid]][carry_cols].reset_index(drop=True)
        for c in take_rows.columns:
            if c in {"Observation"}:
                continue
            res.loc[valid, c] = take_rows[c].to_numpy()

    out.append(res)

sec_by_sec = pd.concat(out, ignore_index=True)

print("sec_by_sec rows:", sec_by_sec.shape[0])
print("unique observations:", sec_by_sec["Observation"].nunique())

# Overlap analysis per date
sec_by_sec["date"] = sec_by_sec["date_time_abs"].dt.date

overlap_counts = (
    sec_by_sec.groupby(["date", "date_time_abs"])["Observation"]
    .nunique()
    .reset_index(name="obs_count")
)

overlap_seconds = overlap_counts[overlap_counts["obs_count"] > 1]
print("Overlapping seconds (count):", overlap_seconds.shape[0])

# Conflict check for overlapping seconds
cols_to_check = [c for c in sec_by_sec.columns if c not in ["date_time_abs", "date", "Observation"]]
if overlap_seconds.empty:
    print("No overlaps found.")
else:
    overlaps = sec_by_sec.merge(overlap_seconds[["date", "date_time_abs"]], on=["date", "date_time_abs"])
    conflicts = overlaps.groupby(["date", "date_time_abs"])[cols_to_check].nunique(dropna=False)
    conflict_cols = [c for c in cols_to_check if conflicts[c].max() > 1]
    print("Columns with conflicts among overlapping observations:", conflict_cols)
    if conflict_cols:
        example_times = conflicts[conflicts[conflict_cols].max(axis=1) > 1].reset_index().head(5)
        print("\nExample conflicting seconds:")
        for _, row in example_times.iterrows():
            t = row["date_time_abs"]
            print("\nTime:", t)
            print(overlaps[overlaps["date_time_abs"] == t][["Observation"] + conflict_cols].head(10))

# Summary per observation
per_obs = (
    sec_by_sec.groupby("Observation")["date_time_abs"]
    .agg(first="min", last="max", rows="size")
    .reset_index()
)
per_obs["duration_seconds"] = (
    per_obs["last"] - per_obs["first"]
).dt.total_seconds()
print("\nPer-observation second-grid summary:")
print(per_obs.sort_values("first"))

# Compare copyA vs copyB per date: are copyA seconds subset of copyB?
print("\nSubset checks (copyA vs copyB) per date:")
for date, g in sec_by_sec.groupby("date"):
    obs_list = sorted(g["Observation"].unique())
    obs_a = [o for o in obs_list if "copyA" in o]
    obs_b = [o for o in obs_list if "copyB" in o]
    if not obs_a or not obs_b:
        continue
    # assume one copyA and one copyB per date
    a = obs_a[0]
    b = obs_b[0]
    a_times = set(g[g["Observation"] == a]["date_time_abs"])
    b_times = set(g[g["Observation"] == b]["date_time_abs"])
    print(f"Date {date}: {a} vs {b}")
    print("  copyA seconds:", len(a_times))
    print("  copyB seconds:", len(b_times))
    print("  copyA subset of copyB:", a_times.issubset(b_times))
    print("  copyB subset of copyA:", b_times.issubset(a_times))
    print("  copyB extra seconds:", len(b_times - a_times))

# Quality metrics: non-null and non-"not coded" counts per observation
quality_cols = [c for c in ["Behavior", "Modifier_1", "Modifier_2", "Modifier_3", "Modifier_4", "Comment"] if c in sec_by_sec.columns]
if quality_cols:
    print("\nQuality metrics per Observation:")
    quality = []
    for obs, g in sec_by_sec.groupby("Observation"):
        row = {"Observation": obs, "rows": len(g)}
        for c in quality_cols:
            non_null = g[c].notna().sum()
            non_nc = g[c].notna() & (g[c].astype(str).str.lower() != "not coded")
            row[f"{c}_non_null"] = int(non_null)
            row[f"{c}_non_not_coded"] = int(non_nc.sum())
        quality.append(row)
    quality_df = pd.DataFrame(quality)
    print(quality_df.sort_values("Observation"))

# Conflict breakdown for overlapping seconds (copyA vs copyB only)
print("\nConflict breakdown on overlapping seconds (copyA vs copyB):")
for date, g in sec_by_sec.groupby("date"):
    obs_list = sorted(g["Observation"].unique())
    obs_a = [o for o in obs_list if "copyA" in o]
    obs_b = [o for o in obs_list if "copyB" in o]
    if not obs_a or not obs_b:
        continue
    a = obs_a[0]
    b = obs_b[0]
    ga = g[g["Observation"] == a].set_index("date_time_abs")
    gb = g[g["Observation"] == b].set_index("date_time_abs")
    overlap_idx = ga.index.intersection(gb.index)
    print(f"\nDate {date}: overlap seconds = {len(overlap_idx)}")
    for c in quality_cols:
        a_vals = ga.loc[overlap_idx, c]
        b_vals = gb.loc[overlap_idx, c]
        a_null = a_vals.isna()
        b_null = b_vals.isna()
        both_null = a_null & b_null
        a_only = (~a_null) & b_null
        b_only = a_null & (~b_null)
        both_non_null = (~a_null) & (~b_null)
        both_diff = both_non_null & (a_vals.astype(str) != b_vals.astype(str))
        print(
            f"  {c}: both_null={both_null.sum()}, "
            f"a_only={a_only.sum()}, b_only={b_only.sum()}, "
            f"both_non_null_diff={both_diff.sum()}"
        )
