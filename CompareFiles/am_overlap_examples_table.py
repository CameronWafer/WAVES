import pandas as pd

XLSX = r"C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx"
OUT_CSV = r"C:/Users/HELIOS-300/Desktop/WAVES/CompareFiles/am_overlap_examples.csv"

df = pd.read_excel(XLSX, engine="openpyxl")
df = df[df["Event_Type"] == "State start"].copy()
df["Time_Absolute_hms"] = pd.to_datetime(df["Time_Absolute_hms"], errors="coerce")
df = df.dropna(subset=["Observation", "Time_Absolute_hms"])

df["id"] = df["Observation"].astype(str).str.extract(r"AM(\d{2})", expand=False).astype("int64")
df["do_session"] = df["Observation"].astype(str).str.extract(r"(DO\d+(?:_[ab])?)", expand=False).str.strip()
df["do_session"] = (
    df["do_session"].str.replace("_A", "_a", regex=False).str.replace("_B", "_b", regex=False).str.replace(r"_(a|b)$", "", regex=True)
)

copy_label = pd.Series("", index=df.index)
copy_label[df["Observation"].astype(str).str.contains("copyA", case=False, na=False)] = "copyA"
copy_label[df["Observation"].astype(str).str.contains("copyB", case=False, na=False)] = "copyB"
df["copy_label"] = copy_label

obs_ranges = (
    df[df["copy_label"].isin(["copyA", "copyB"])]
    .groupby("Observation", as_index=False)
    .agg(
        id=("id", "first"),
        do_session=("do_session", "first"),
        copy_label=("copy_label", "first"),
        start=("Time_Absolute_hms", "min"),
        end=("Time_Absolute_hms", "max"),
    )
)

rows = []
for (sid, sdo), g in obs_ranges.groupby(["id", "do_session"], sort=True):
    a = g[g["copy_label"] == "copyA"].sort_values("start").reset_index(drop=True)
    b = g[g["copy_label"] == "copyB"].sort_values("start").reset_index(drop=True)
    n = min(len(a), len(b))
    for i in range(n):
        a_start, a_end = a.loc[i, "start"], a.loc[i, "end"]
        b_start, b_end = b.loc[i, "start"], b.loc[i, "end"]

        ov_start = max(a_start, b_start)
        ov_end = min(a_end, b_end)
        has_overlap = ov_start <= ov_end

        rows.append(
            {
                "id": sid,
                "do_session": sdo,
                "pair_index": i + 1,
                "date_copyA": a_start.date().isoformat(),
                "date_copyB": b_start.date().isoformat(),
                "copyA_observation": a.loc[i, "Observation"],
                "copyB_observation": b.loc[i, "Observation"],
                "copyA_start": a_start.strftime("%Y-%m-%d %H:%M:%S"),
                "copyA_end": a_end.strftime("%Y-%m-%d %H:%M:%S"),
                "copyB_start": b_start.strftime("%Y-%m-%d %H:%M:%S"),
                "copyB_end": b_end.strftime("%Y-%m-%d %H:%M:%S"),
                "overlap_start": ov_start.strftime("%Y-%m-%d %H:%M:%S") if has_overlap else "",
                "overlap_end": ov_end.strftime("%Y-%m-%d %H:%M:%S") if has_overlap else "",
                "overlap_seconds": int((ov_end - ov_start).total_seconds() + 1) if has_overlap else 0,
            }
        )

out = pd.DataFrame(rows).sort_values(["id", "do_session", "pair_index"]).reset_index(drop=True)
out.to_csv(OUT_CSV, index=False)

print(f"Saved overlap table: {OUT_CSV}")
print(out.to_string(index=False))
