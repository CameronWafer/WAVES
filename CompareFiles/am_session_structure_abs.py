"""
Temp analysis: understand duplicate-causing session structure using absolute time.

This script answers:
1) Which (id, do_base) have multiple Observations?
2) Are those Observations overlapping or separated by gaps (absolute time)?
3) Which cases are copyA/copyB vs non-copy multi-observation sessions?
"""

import pandas as pd

XLSX = r"C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx"

df = pd.read_excel(XLSX, engine="openpyxl")
df = df[df["Event_Type"] == "State start"].copy()

time_col = "Time_Absolute_hms"
df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
df = df.dropna(subset=["Observation", time_col])

df["id"] = df["Observation"].astype(str).str.extract(r"AM(\d{2})", expand=False).astype("int64")
df["do"] = df["Observation"].astype(str).str.extract(r"(DO\d+(?:_[ab])?)", expand=False).str.strip()
df["do_base"] = (
    df["do"].str.replace("_A", "_a", regex=False).str.replace("_B", "_b", regex=False).str.replace(r"_(a|b)$", "", regex=True)
)

obs = (
    df.groupby("Observation", as_index=False)
    .agg(
        id=("id", "first"),
        do_base=("do_base", "first"),
        start=(time_col, "min"),
        end=(time_col, "max"),
        rows=("Observation", "size"),
    )
)
obs["duration_sec"] = (obs["end"] - obs["start"]).dt.total_seconds() + 1
obs["copy_flag"] = "none"
obs.loc[obs["Observation"].str.contains("copyA", case=False, na=False), "copy_flag"] = "copyA"
obs.loc[obs["Observation"].str.contains("copyB", case=False, na=False), "copy_flag"] = "copyB"


def pair_overlap_gap(a_start, a_end, b_start, b_end):
    overlap_start = max(a_start, b_start)
    overlap_end = min(a_end, b_end)
    overlap = 0
    if overlap_start <= overlap_end:
        overlap = (overlap_end - overlap_start).total_seconds() + 1

    # Positive gap only when disjoint
    gap = 0
    if a_end < b_start:
        gap = (b_start - a_end).total_seconds() - 1
    elif b_end < a_start:
        gap = (a_start - b_end).total_seconds() - 1
    return int(max(overlap, 0)), int(max(gap, 0))


rows = []
for (sid, sdo), g in obs.groupby(["id", "do_base"], sort=True):
    g = g.sort_values("start").reset_index(drop=True)
    if len(g) == 1:
        rows.append(
            {
                "id": sid,
                "do_base": sdo,
                "n_observations": 1,
                "obs_names": g.loc[0, "Observation"],
                "copy_mix": g.loc[0, "copy_flag"],
                "span_start": g["start"].min(),
                "span_end": g["end"].max(),
                "span_sec": int((g["end"].max() - g["start"].min()).total_seconds() + 1),
                "sum_obs_sec": int(g["duration_sec"].sum()),
                "pairwise_overlap_sec": 0,
                "consecutive_gap_sec": 0,
            }
        )
        continue

    pairwise_overlap = 0
    for i in range(len(g)):
        for j in range(i + 1, len(g)):
            ov, _ = pair_overlap_gap(g.loc[i, "start"], g.loc[i, "end"], g.loc[j, "start"], g.loc[j, "end"])
            pairwise_overlap += ov

    consecutive_gap = 0
    for i in range(len(g) - 1):
        _, gp = pair_overlap_gap(g.loc[i, "start"], g.loc[i, "end"], g.loc[i + 1, "start"], g.loc[i + 1, "end"])
        consecutive_gap += gp

    rows.append(
        {
            "id": sid,
            "do_base": sdo,
            "n_observations": len(g),
            "obs_names": " | ".join(g["Observation"].tolist()),
            "copy_mix": ", ".join(sorted(set(g["copy_flag"].tolist()))),
            "span_start": g["start"].min(),
            "span_end": g["end"].max(),
            "span_sec": int((g["end"].max() - g["start"].min()).total_seconds() + 1),
            "sum_obs_sec": int(g["duration_sec"].sum()),
            "pairwise_overlap_sec": int(pairwise_overlap),
            "consecutive_gap_sec": int(consecutive_gap),
        }
    )

session = pd.DataFrame(rows).sort_values(["id", "do_base"]).reset_index(drop=True)

multi = session[session["n_observations"] > 1].copy()
multi["dup_from_overlap_est"] = multi["sum_obs_sec"] - (multi["span_sec"] - multi["consecutive_gap_sec"])

print("\n=== Multi-Observation Sessions (absolute-time structure) ===")
print(
    multi[
        [
            "id",
            "do_base",
            "n_observations",
            "copy_mix",
            "span_sec",
            "sum_obs_sec",
            "pairwise_overlap_sec",
            "consecutive_gap_sec",
            "dup_from_overlap_est",
        ]
    ].to_string(index=False)
)

print("\n=== Detailed names for multi-observation sessions ===")
for _, r in multi.iterrows():
    print(f"\nAM{int(r['id']):02d} {r['do_base']}:")
    print(r["obs_names"])

print("\n=== High-level counts ===")
print(f"Total sessions: {len(session)}")
print(f"Sessions with >1 Observation: {len(multi)}")
print(f"Sessions with copy labels present: {(multi['copy_mix'] != 'none').sum()}")
print(f"Sessions with NO copy labels but still >1 Observation: {(multi['copy_mix'] == 'none').sum()}")

