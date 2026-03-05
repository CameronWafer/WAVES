import re
from pathlib import Path

import numpy as np
import pandas as pd


BEHAV_PATH = Path("C:/Users/HELIOS-300/Downloads/ACT24_behposture_event(in).csv")
LOG_PATH = Path("C:/Users/HELIOS-300/Downloads/do_log_final_behavior(in).csv")
STEPS_PATH = Path("C:/Users/HELIOS-300/Desktop/Data/seconds_ground_truth_20250410.csv")
OUT_DIR = Path("C:/Users/HELIOS-300/Desktop/WAVES/CompareFiles/act24_diag")


def _standardize_rel_time(time_str: str) -> str:
    s = str(time_str).strip()
    parts = s.split(":")
    if len(parts) == 3:
        return f"{parts[0].zfill(2)}:{parts[1]}:{parts[2]}"
    return s


def _parse_hms_to_seconds(series: pd.Series) -> pd.Series:
    return pd.to_timedelta(series.astype(str).str.strip(), errors="coerce").dt.total_seconds()


def _format_hms(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _normalize_behavior(value: object) -> str | None:
    if pd.isna(value):
        return None
    s = str(value).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


DOMAIN_PREFIXES = {
    "sl-",
    "pc-",
    "ha-",
    "ca-",
    "wrk-",
    "edu-",
    "org-",
    "pur-",
    "eat-",
    "les-",
    "ex-",
    "trav-",
    "other-",
}
POSTURE_PREFIXES = {"sb-", "la-", "wa-", "sp-"}


def _classify_behavior(value: object) -> str:
    s = _normalize_behavior(value)
    if not s:
        return "other"
    for p in DOMAIN_PREFIXES:
        if s.startswith(p):
            return "activity"
    for p in POSTURE_PREFIXES:
        if s.startswith(p):
            return "posture"
    return "other"


def _expand_track_to_seconds(track_df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for _, g in track_df.groupby("Observation", sort=False):
        g = g.copy()
        g = g[g["_seconds"].notna()]
        if g.empty:
            continue
        g["_event_second"] = np.floor(g["_seconds"]).astype(int)
        g_last = (
            g.sort_values(["_event_second", "_seconds"], kind="mergesort")
            .drop_duplicates(subset=["_event_second"], keep="last")
        )
        min_s = int(np.floor(float(g["_seconds"].min())))
        max_start_s = int(np.floor(float(g["_seconds"].max())))
        dur = pd.to_numeric(g.get("Duration_sf", pd.Series([0] * len(g))), errors="coerce").fillna(0.0)
        max_end = np.floor((g["_seconds"] + dur).max())
        max_s = int(max(max_start_s, max_end))
        full_index = np.arange(min_s, max_s + 1, dtype=int)
        aligned = g_last.set_index("_event_second").sort_index().reindex(full_index).ffill()
        take = aligned.copy().reset_index(drop=False).rename(columns={"_event_second": "_second"})
        take["rel_time"] = take["_second"].apply(_format_hms)
        out.append(take)
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)


def build_joined_pre_steps() -> pd.DataFrame:
    beh = pd.read_csv(BEHAV_PATH)
    log = pd.read_csv(LOG_PATH)

    beh_s = beh[beh["Event_Type"].eq("State start")].copy()
    parts = beh_s["Observation"].astype(str).str.split("_", expand=True)
    beh_s["id"] = pd.to_numeric(parts[1], errors="coerce")
    beh_s["obs"] = pd.to_numeric(parts[2], errors="coerce")
    beh_s["do"] = beh_s["obs"]
    beh_s["_seconds"] = _parse_hms_to_seconds(beh_s["Time_Relative_hms"])
    beh_s["rel_time"] = beh_s["Time_Relative_hms"].astype(str).str.strip().map(_standardize_rel_time)
    beh_s["_track"] = beh_s["Behavior"].apply(_classify_behavior)

    log2 = log.copy()
    log2["date"] = pd.to_datetime(
        {
            "year": pd.to_numeric(log2["start_year"], errors="coerce"),
            "month": pd.to_numeric(log2["start_month"], errors="coerce"),
            "day": pd.to_numeric(log2["start_day"], errors="coerce"),
        },
        errors="coerce",
    ).dt.strftime("%m/%d/%Y")
    log2 = log2.loc[:, ["id", "do", "date", "start_time"]].copy()
    log2["id"] = pd.to_numeric(log2["id"], errors="coerce")
    log2["do"] = pd.to_numeric(log2["do"], errors="coerce")
    log2 = log2.sort_values(["id", "do"]).drop_duplicates(subset=["id", "do"], keep="first")

    b = beh_s.merge(log2[["id", "do", "start_time"]], on=["id", "do"], how="left", validate="many_to_one")
    b["start_time_dt"] = pd.to_datetime(
        b["start_time"].astype(str).str.strip(), format="%I:%M:%S %p", errors="coerce"
    ).fillna(pd.to_datetime(b["start_time"].astype(str).str.strip(), format="%I:%M %p", errors="coerce"))
    b["time_relative_td"] = pd.to_timedelta(b["Time_Relative_hmsf"].astype(str).str.strip(), errors="coerce")
    b["start_time_new"] = (b["start_time_dt"] + b["time_relative_td"]).dt.strftime("%I:%M:%S %p")

    activity = b[b["_track"].eq("activity")].copy()
    posture = b[b["_track"].eq("posture")].copy()
    activity_exp = _expand_track_to_seconds(activity)
    posture_exp = _expand_track_to_seconds(posture)

    activity_subset = activity_exp[
        ["Observation", "_second", "start_time_new", "id", "do"]
    ].drop_duplicates(subset=["Observation", "_second"], keep="last")
    posture_subset = posture_exp[
        ["Observation", "_second"]
    ].drop_duplicates(subset=["Observation", "_second"], keep="last")

    merged = activity_subset.merge(posture_subset, on=["Observation", "_second"], how="outer")
    merged = merged.sort_values(["Observation", "_second"], kind="mergesort")
    for c in ["id", "do", "start_time_new"]:
        merged[c] = merged.groupby("Observation")[c].ffill().bfill()

    merged["rel_time"] = merged["_second"].astype(int).apply(_format_hms)
    merged["id"] = merged["id"].astype("Int64")
    merged["obs"] = merged["do"].astype("Int64")

    joined = merged.merge(log2[["id", "do", "date"]].rename(columns={"do": "obs"}), on=["id", "obs"], how="left")
    joined["date_time"] = np.where(
        joined["start_time_new"].notna(),
        joined["date"].astype(str).str.strip() + " " + joined["start_time_new"].astype(str).str.strip(),
        np.nan,
    )
    joined["id"] = joined["id"].astype(str).str.strip()
    joined["obs"] = joined["obs"].astype(str).str.strip()
    joined["rel_time"] = joined["rel_time"].astype(str).str.strip().map(_standardize_rel_time)
    return joined


def _key_counts(df: pd.DataFrame) -> pd.Series:
    return df[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).value_counts()


def evaluate_strategy(name: str, joined: pd.DataFrame, steps_variant: pd.DataFrame) -> dict:
    m = joined.merge(steps_variant[["id", "obs", "rel_time", "Quality", "Step"]], on=["id", "obs", "rel_time"], how="left")
    vc = _key_counts(m)
    dt_vc = m[m["date_time"].notna()][["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1).value_counts()
    steps_num = pd.to_numeric(m["Step"], errors="coerce")
    return {
        "strategy": name,
        "rows_after_merge": int(len(m)),
        "dup_id_obs_rel_time_keys": int((vc > 1).sum()),
        "dup_id_obs_rel_time_max_mult": int(vc.max()),
        "dup_id_obs_date_time_keys": int((dt_vc > 1).sum()),
        "dup_id_obs_date_time_max_mult": int(dt_vc.max()) if len(dt_vc) else 0,
        "matched_step_rows": int(steps_num.notna().sum()),
        "step_sum": float(steps_num.sum(skipna=True)),
        "step_mean": float(steps_num.mean(skipna=True)),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    joined = build_joined_pre_steps()
    steps = pd.read_csv(STEPS_PATH).rename(columns={"ID": "id", "Session": "obs", "relative_time_steps": "rel_time"})
    steps["id"] = steps["id"].astype(str).str.strip()
    steps["obs"] = steps["obs"].astype(str).str.strip()
    steps["rel_time"] = steps["rel_time"].astype(str).str.strip().map(_standardize_rel_time)

    # Duplicate-key profile
    key_vc = _key_counts(steps)
    dup_keys = key_vc[key_vc > 1]
    dup_rows = steps[steps[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).isin(dup_keys.index)].copy()
    dup_rows["_key"] = dup_rows[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1)
    conflict = dup_rows.groupby("_key").agg(
        n_rows=("Step", "size"),
        step_nunique=("Step", "nunique"),
        quality_nunique=("Quality", "nunique"),
        min_step=("Step", "min"),
        max_step=("Step", "max"),
    ).reset_index()
    conflict.to_csv(OUT_DIR / "act24_steps_duplicate_conflicts.csv", index=False)

    # Build candidate step tables
    raw = steps.copy()
    exact_dedup = steps.drop_duplicates(subset=["id", "obs", "rel_time", "Quality", "Step"]).copy()

    keep_first = (
        steps.sort_values(["id", "obs", "rel_time"], kind="mergesort")
        .drop_duplicates(subset=["id", "obs", "rel_time"], keep="first")
        .copy()
    )

    # Prefer Codable when quality differs; tie-break by larger Step
    q_rank = {"Codable": 1, "Non-codeable": 0}
    qc = steps.copy()
    qc["_qrank"] = qc["Quality"].map(q_rank).fillna(-1)
    qc["_step_num"] = pd.to_numeric(qc["Step"], errors="coerce").fillna(-1)
    pref_codable = (
        qc.sort_values(["id", "obs", "rel_time", "_qrank", "_step_num"], ascending=[True, True, True, False, False], kind="mergesort")
        .drop_duplicates(subset=["id", "obs", "rel_time"], keep="first")
        .drop(columns=["_qrank", "_step_num"])
        .copy()
    )

    agg_max = (
        steps.assign(_step_num=pd.to_numeric(steps["Step"], errors="coerce"))
        .groupby(["id", "obs", "rel_time"], as_index=False)
        .agg(Quality=("Quality", "first"), Step=("_step_num", "max"))
    )
    agg_sum = (
        steps.assign(_step_num=pd.to_numeric(steps["Step"], errors="coerce"))
        .groupby(["id", "obs", "rel_time"], as_index=False)
        .agg(Quality=("Quality", "first"), Step=("_step_num", "sum"))
    )

    metrics = [
        evaluate_strategy("raw", joined, raw),
        evaluate_strategy("exact_dedup", joined, exact_dedup),
        evaluate_strategy("keep_first", joined, keep_first),
        evaluate_strategy("prefer_codable_then_max_step", joined, pref_codable),
        evaluate_strategy("aggregate_max_step", joined, agg_max),
        evaluate_strategy("aggregate_sum_step", joined, agg_sum),
    ]
    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv(OUT_DIR / "act24_steps_strategy_metrics.csv", index=False)

    size_df = pd.DataFrame(
        [
            {"table": "steps_raw", "rows": len(raw), "dup_keys": int((_key_counts(raw) > 1).sum())},
            {"table": "steps_exact_dedup", "rows": len(exact_dedup), "dup_keys": int((_key_counts(exact_dedup) > 1).sum())},
            {"table": "steps_keep_first", "rows": len(keep_first), "dup_keys": int((_key_counts(keep_first) > 1).sum())},
            {"table": "steps_prefer_codable", "rows": len(pref_codable), "dup_keys": int((_key_counts(pref_codable) > 1).sum())},
            {"table": "steps_agg_max", "rows": len(agg_max), "dup_keys": int((_key_counts(agg_max) > 1).sum())},
            {"table": "steps_agg_sum", "rows": len(agg_sum), "dup_keys": int((_key_counts(agg_sum) > 1).sum())},
        ]
    )
    size_df.to_csv(OUT_DIR / "act24_steps_strategy_table_sizes.csv", index=False)

    print("Wrote:")
    print("-", OUT_DIR / "act24_steps_duplicate_conflicts.csv")
    print("-", OUT_DIR / "act24_steps_strategy_metrics.csv")
    print("-", OUT_DIR / "act24_steps_strategy_table_sizes.csv")


if __name__ == "__main__":
    main()
