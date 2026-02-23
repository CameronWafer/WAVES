"""
Temp analysis: decision table for combining sessions with potential duplicates.
Uses exported Cameron_AM_Clean.csv full date_time.
"""
import pandas as pd

CSV = r"C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv"
df = pd.read_csv(CSV, low_memory=False)
df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")
df = df.dropna(subset=["date_time", "id", "do_session"])
df["id"] = df["id"].astype(int)
df["date"] = df["date_time"].dt.date

records = []
for (sid, sdo), g in df.groupby(["id", "do_session"], sort=True):
    rows = len(g)
    uniq_dt = g["date_time"].nunique()
    dup_dt = rows - uniq_dt
    uniq_time = g["time"].nunique()
    dup_time = rows - uniq_time
    dates = sorted(g["date"].astype(str).unique().tolist())
    n_dates = len(dates)

    if dup_dt > 0 and n_dates == 1:
        decision = "Needs overlap merge rule (copyA/copyB style)"
        reason = "True same-second duplicates on same date"
    elif dup_dt > 0 and n_dates > 1:
        decision = "Split by date first, then overlap merge rule"
        reason = "True same-second duplicates + multiple dates"
    elif dup_dt == 0 and dup_time > 0 and n_dates > 1:
        decision = "Can combine if keyed by full date_time (not time-only)"
        reason = "No true datetime duplicates; repeated clock times across dates"
    else:
        decision = "Already seamless by full date_time"
        reason = "No duplicate seconds"

    records.append(
        {
            "id": sid,
            "do_session": sdo,
            "rows": rows,
            "dup_by_date_time": dup_dt,
            "dup_by_time_only": dup_time,
            "n_dates": n_dates,
            "dates": " | ".join(dates),
            "decision": decision,
            "reason": reason,
        }
    )

out = pd.DataFrame(records).sort_values(["id", "do_session"]).reset_index(drop=True)

print("\n=== Combine Decision Table ===")
print(
    out[
        [
            "id",
            "do_session",
            "rows",
            "dup_by_date_time",
            "dup_by_time_only",
            "n_dates",
            "decision",
        ]
    ].to_string(index=False)
)

print("\n=== Sessions requiring action (not already seamless) ===")
action = out[out["decision"] != "Already seamless by full date_time"]
print(action[["id", "do_session", "n_dates", "dup_by_date_time", "dup_by_time_only", "decision", "reason"]].to_string(index=False))

