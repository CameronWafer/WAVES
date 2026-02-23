"""
Temp analysis: compare duplicate counts by `time` vs full `date_time`.
This checks whether "duplicates" come from same clock time on different dates.
"""

import pandas as pd

CSV = r"C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv"

df = pd.read_csv(CSV)
df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")
df = df.dropna(subset=["date_time", "id", "do_session"])
df["id_int"] = df["id"].astype(int)
df["date"] = df["date_time"].dt.date

rows = []
for (sid, sdo), g in df.groupby(["id_int", "do_session"], sort=True):
    row_count = len(g)

    uniq_dt = g["date_time"].nunique()
    dup_dt = row_count - uniq_dt

    uniq_time = g["time"].nunique() if "time" in g.columns else g["date_time"].dt.strftime("%H:%M:%S").nunique()
    dup_time = row_count - uniq_time

    uniq_dates = g["date"].nunique()
    dates = sorted(str(d) for d in g["date"].dropna().unique())

    rows.append(
        {
            "id": sid,
            "do_session": sdo,
            "rows": row_count,
            "unique_date_time": uniq_dt,
            "duplicate_by_date_time": dup_dt,
            "unique_time_hms": uniq_time,
            "duplicate_by_time_hms": dup_time,
            "n_dates": uniq_dates,
            "dates": " | ".join(dates[:4]) + (" ..." if len(dates) > 4 else ""),
        }
    )

out = pd.DataFrame(rows).sort_values(["id", "do_session"]).reset_index(drop=True)

print("\n=== Duplicate Comparison: full date_time vs time-only ===")
print(
    out[
        [
            "id",
            "do_session",
            "rows",
            "duplicate_by_date_time",
            "duplicate_by_time_hms",
            "n_dates",
            "dates",
        ]
    ].to_string(index=False)
)

print("\n=== Sessions where duplicate_by_time_hms > 0 but duplicate_by_date_time == 0 ===")
mask = (out["duplicate_by_time_hms"] > 0) & (out["duplicate_by_date_time"] == 0)
print(out.loc[mask, ["id", "do_session", "rows", "duplicate_by_time_hms", "n_dates", "dates"]].to_string(index=False))

print("\n=== Sessions where duplicate_by_date_time > 0 (true same-second duplicates) ===")
mask2 = out["duplicate_by_date_time"] > 0
print(out.loc[mask2, ["id", "do_session", "rows", "duplicate_by_date_time", "n_dates", "dates"]].to_string(index=False))

