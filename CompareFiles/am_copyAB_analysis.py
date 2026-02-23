"""
Analysis of copyA / copyB Observations: time ranges, overlap, gaps.
Based on TIME_ABSOLUTE (wall-clock) only, NOT time_relative.
Goal: determine whether copyA and copyB can be combined seamlessly per (id, DO_session)
or must be kept separate.
"""
import pandas as pd
import numpy as np

PATH_XLSX = r"C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx"

print("Loading behavior data (State start only)...")
df = pd.read_excel(PATH_XLSX, engine="openpyxl")
df = df[df["Event_Type"] == "State start"].copy()

# Use Time_Absolute_hms (wall-clock) only — not Time_Relative_hms
time_col = "Time_Absolute_hms"
df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
df = df.dropna(subset=["Observation", time_col])
print(f"Analysis time column: {time_col} (absolute/wall-clock only, not relative).")

# Parse Observation -> id, do, copy_label
df["id"] = df["Observation"].astype(str).str.extract(r"AM(\d{2})", expand=False).astype("int64")
df["do"] = df["Observation"].astype(str).str.extract(r"(DO\d+(?:_[ab])?)", expand=False).str.strip()
df["do"] = df["do"].str.replace("_A", "_a", regex=False).str.replace("_B", "_b", regex=False)
df["do_base"] = df["do"].str.replace(r"_(a|b)$", "", regex=True)
# Copy label: "copyA", "copyB", or "" (none)
df["copy_label"] = ""
df.loc[df["Observation"].astype(str).str.contains("copyA", case=False, na=False), "copy_label"] = "copyA"
df.loc[df["Observation"].astype(str).str.contains("copyB", case=False, na=False), "copy_label"] = "copyB"

# Time range per Observation (from Time_Absolute_hms only)
obs_ranges = (
    df.groupby("Observation", as_index=False)
    .agg(
        id=("id", "first"),
        do=("do", "first"),
        do_base=("do_base", "first"),
        copy_label=("copy_label", "first"),
        start=(time_col, "min"),
        end=(time_col, "max"),
        row_count=(time_col, "count"),
    )
)
obs_ranges["duration_sec"] = (obs_ranges["end"] - obs_ranges["start"]).dt.total_seconds() + 1  # inclusive seconds

# Focus: (id, do_base) that have at least one copyA or copyB
has_copy = obs_ranges["copy_label"].isin(["copyA", "copyB"])
copy_obs = obs_ranges[has_copy]
session_keys = copy_obs.groupby(["id", "do_base"]).size().reset_index(name="n_obs")
session_keys = session_keys[session_keys["n_obs"] >= 1]

print("\n" + "=" * 80)
print("1. OBSERVATIONS WITH copyA / copyB (time ranges from Time_Absolute_hms only)")
print("=" * 80)

# Show all copyA/copyB Observations with their time ranges
copy_display = copy_obs[["id", "do_base", "Observation", "copy_label", "start", "end", "duration_sec", "row_count"]].copy()
copy_display["start_str"] = copy_display["start"].dt.strftime("%Y-%m-%d %H:%M:%S")
copy_display["end_str"] = copy_display["end"].dt.strftime("%Y-%m-%d %H:%M:%S")
copy_display["duration_hms"] = pd.to_timedelta(copy_display["duration_sec"], unit="s").astype(str).str.replace("0 days ", "", regex=False)
for (iid, do_base), g in copy_display.groupby(["id", "do_base"], sort=False):
    print(f"\n--- id={iid} {do_base} ---")
    for _, r in g.sort_values("start").iterrows():
        print(f"  {r['copy_label']:6}  {r['Observation'][:35]:35}  {r['start_str']} -> {r['end_str']}  dur={r['duration_hms']}  rows={r['row_count']}")

print("\n" + "=" * 80)
print("2. OVERLAP & GAP PER (id, do_base) from Time_Absolute_hms (absolute time only)")
print("=" * 80)

results = []
for (iid, do_base), g in obs_ranges[obs_ranges["copy_label"].isin(["copyA", "copyB"])].groupby(["id", "do_base"], sort=False):
    g = g.sort_values("start").reset_index(drop=True)
    obs_list = g["Observation"].tolist()
    starts = g["start"].values
    ends = g["end"].values
    labels = g["copy_label"].tolist()

    total_span_start = starts.min()
    total_span_end = ends.max()
    total_span_sec = (pd.Timestamp(total_span_end) - pd.Timestamp(total_span_start)).total_seconds() + 1

    # Overlap: sum of pairwise overlaps (max(0, min(e1,e2) - max(s1,s2) + 1) for each pair)
    overlap_sec = 0
    for i in range(len(g)):
        for j in range(i + 1, len(g)):
            o_start = max(starts[i], starts[j])
            o_end = min(ends[i], ends[j])
            if o_start <= o_end:
                overlap_sec += (pd.Timestamp(o_end) - pd.Timestamp(o_start)).total_seconds() + 1

    # Gap: union of segments, then expected seconds minus sum of segment lengths (simplified: sum of gaps between consecutive segments when sorted by start)
    # Consecutive segments (by start): gap between end[i] and start[i+1]
    gap_sec = 0
    sorted_idx = np.argsort(starts)
    for k in range(len(sorted_idx) - 1):
        end_first = ends[sorted_idx[k]]
        start_next = starts[sorted_idx[k + 1]]
        delta = (pd.Timestamp(start_next) - pd.Timestamp(end_first)).total_seconds() - 1  # gap = seconds between (exclusive)
        if delta > 0:
            gap_sec += delta

    # Unique seconds if we take union of all segments (approximation: total_span - gap, or sum of durations - overlap)
    sum_duration = g["duration_sec"].sum()
    unique_sec_approx = sum_duration - overlap_sec  # union of intervals

    results.append({
        "id": iid,
        "do_base": do_base,
        "n_observations": len(g),
        "observations": " | ".join(obs_list),
        "copy_labels": ", ".join(labels),
        "first_start": total_span_start,
        "last_end": total_span_end,
        "total_span_seconds": total_span_sec,
        "overlap_seconds": overlap_sec,
        "gap_seconds": gap_sec,
        "sum_duration_sec": sum_duration,
        "unique_sec_approx": unique_sec_approx,
    })

res_df = pd.DataFrame(results)
res_df["overlap_hms"] = pd.to_timedelta(res_df["overlap_seconds"], unit="s").astype(str).str.replace("0 days ", "", regex=False)
res_df["gap_hms"] = pd.to_timedelta(res_df["gap_seconds"], unit="s").astype(str).str.replace("0 days ", "", regex=False)

pd.set_option("display.max_colwidth", 50)
pd.set_option("display.width", 200)
print(res_df[["id", "do_base", "n_observations", "copy_labels", "overlap_seconds", "overlap_hms", "gap_seconds", "gap_hms", "unique_sec_approx"]].to_string(index=False))

print("\n" + "=" * 80)
print("3. PATTERN SUMMARY")
print("=" * 80)

n_overlap = (res_df["overlap_seconds"] > 0).sum()
n_gap = (res_df["gap_seconds"] > 0).sum()
n_both = ((res_df["overlap_seconds"] > 0) & (res_df["gap_seconds"] > 0)).sum()
n_clean_split = ((res_df["overlap_seconds"] == 0) & (res_df["gap_seconds"] == 0)).sum()
n_adjacent = ((res_df["overlap_seconds"] == 0) & (res_df["gap_seconds"] == 0) & (res_df["n_observations"] == 2)).sum()

print(f"Sessions with copyA/copyB: {len(res_df)}")
print(f"  - Overlap (shared seconds):     {n_overlap} sessions")
print(f"  - Gap (time between segments):   {n_gap} sessions")
print(f"  - Both overlap and gap:          {n_both} sessions")
print(f"  - No overlap and no gap (exactly 2 segments, adjacent?): {n_clean_split}")

print("\n" + "=" * 80)
print("4. RECOMMENDATION")
print("=" * 80)
if n_overlap > 0:
    print("""
OVERLAP is present: copyA and copyB often cover the SAME clock seconds (same session
split into two coders/recordings). You cannot combine them without a rule:
  - Option A: Keep separate (one row per Observation per second). Downstream deduplicates
    by (id, do_session, date_time) with a rule (e.g. prefer copyB over copyA for overlapping seconds).
  - Option B: Merge in pipeline: build one timeline per (id, do_base); for each second,
    assign behavior from copyB if both cover it, else copyA (or the other way).
  - Option C: Use only one copy (e.g. copyB) per (id, do_base) and drop copyA.
""")
if n_gap > 0:
    print("""
GAP is present: between copyA end and copyB start there are missing seconds. If you
combine, you get one timeline with a gap (no behavior for those seconds). That is
seamless only if you accept missing data in the gap.
""")
print("""
CONCLUSION:
- Seamless combine (concatenate copyA then copyB with no overlap, no gap): RARE or only
  for some sessions. Your summary showed duplicate_rows > 0 for most copyA/copyB sessions.
- Prefer: either (1) keep separate and document grain = Observation, or (2) merge with
  an explicit rule for overlapping seconds (e.g. copyB overwrites copyA) and accept gaps
  where they exist.
""")

# Optional: per-session detail for overlap
print("\n" + "=" * 80)
print("5. DETAIL: Overlap and gap by session (for merge rule design)")
print("=" * 80)
for _, r in res_df.iterrows():
    print(f"\nid={r['id']} {r['do_base']}: {r['n_observations']} segments, overlap={r['overlap_seconds']:.0f}s ({r['overlap_hms']}), gap={r['gap_seconds']:.0f}s ({r['gap_hms']})")
    if r["overlap_seconds"] > 0:
        print("  -> Overlap: use one copy per second (e.g. prefer copyB) or keep both and deduplicate later.")

print("\nDone.")
