import re
from pathlib import Path

import numpy as np
import pandas as pd


BEHAV_PATH = Path("C:/Users/HELIOS-300/Downloads/ACT24_behposture_event(in).csv")
LOG_PATH = Path("C:/Users/HELIOS-300/Downloads/do_log_final_behavior(in).csv")
STEPS_PATH = Path("C:/Users/HELIOS-300/Desktop/Data/seconds_ground_truth_20250410.csv")
ACT_CLEAN_PATH = Path("C:/Users/HELIOS-300/Desktop/WAVES/ACT24 Full Code/Cameron_ACT24_Clean.csv")
ACT_WAVESREADY_PATH = Path(
    "C:/Users/HELIOS-300/Desktop/WAVES/ACT24 Full Code/Cameron_ACT24_Clean_WavesReady.csv"
)
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
    if s in {"private/not coded", "start posture", "start behavior"}:
        return "other"
    return "other"


def _stage_metrics(name: str, df: pd.DataFrame) -> dict:
    out = {"stage": name, "rows": int(len(df))}
    for keys, label in [
        (["id", "obs", "rel_time"], "dup_id_obs_rel_time_keys"),
        (["id", "obs", "date_time"], "dup_id_obs_date_time_keys"),
    ]:
        if all(c in df.columns for c in keys):
            base = df.copy()
            if "date_time" in keys:
                base = base[base["date_time"].astype(str).str.strip().str.lower().ne("nan")]
            vc = base[keys].astype(str).agg("|".join, axis=1).value_counts()
            out[label] = int((vc > 1).sum())
            out[f"{label}_max_mult"] = int(vc.max()) if len(vc) else 0
        else:
            out[label] = np.nan
            out[f"{label}_max_mult"] = np.nan
    if "posture_wbm" in df.columns and "intensity_do" in df.columns:
        post = df["posture_wbm"].astype(str).str.strip()
        inten = df["intensity_do"].astype(str).str.strip()
        miss = (post != "") & (~post.str.lower().isin(["nan"])) & (
            inten.eq("") | inten.str.lower().isin(["nan", "none", "<na>"])
        )
        out["missing_intensity_with_posture"] = int(miss.sum())
    else:
        out["missing_intensity_with_posture"] = np.nan
    return out


def _expand_track_to_seconds(track_df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for obs, g in track_df.groupby("Observation", sort=False):
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
        if "Duration_sf" in g.columns:
            dur = pd.to_numeric(g["Duration_sf"], errors="coerce").fillna(0.0)
            max_end = np.floor((g["_seconds"] + dur).max())
            max_s = int(max(max_start_s, max_end))
        else:
            max_s = max_start_s
        full_index = np.arange(min_s, max_s + 1, dtype=int)
        aligned = g_last.set_index("_event_second").sort_index().reindex(full_index).ffill()
        take = aligned.copy()
        take.reset_index(drop=False, inplace=True)
        take.rename(columns={"_event_second": "_second"}, inplace=True)
        take["rel_time"] = take["_second"].apply(_format_hms)
        out.append(take)
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    beh = pd.read_csv(BEHAV_PATH)
    log = pd.read_csv(LOG_PATH)
    steps = pd.read_csv(STEPS_PATH)
    act_clean = pd.read_csv(ACT_CLEAN_PATH, dtype=str, keep_default_na=False)
    act_waves = pd.read_csv(ACT_WAVESREADY_PATH, dtype=str, keep_default_na=False)

    baseline_rows = []
    baseline_rows.append(_stage_metrics("act_clean_output", act_clean))
    baseline_rows.append(_stage_metrics("act_wavesready_output", act_waves))

    # Source-level checks
    beh_s = beh[beh["Event_Type"].eq("State start")].copy()
    parts = beh_s["Observation"].astype(str).str.split("_", expand=True)
    beh_s["id"] = pd.to_numeric(parts[1], errors="coerce")
    beh_s["obs"] = pd.to_numeric(parts[2], errors="coerce")
    beh_s["do"] = beh_s["obs"]
    beh_s["rel_time"] = beh_s["Time_Relative_hms"].astype(str).str.strip().map(_standardize_rel_time)
    baseline_rows.append(_stage_metrics("source_state_start", beh_s))

    # Stage replay (high-fidelity approximation)
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

    b = beh_s.copy()
    b = b.merge(log2[["id", "do", "start_time"]], on=["id", "do"], how="left", validate="many_to_one")
    b["start_time_dt"] = pd.to_datetime(
        b["start_time"].astype(str).str.strip(), format="%I:%M:%S %p", errors="coerce"
    ).fillna(pd.to_datetime(b["start_time"].astype(str).str.strip(), format="%I:%M %p", errors="coerce"))
    b["time_relative_td"] = pd.to_timedelta(b["Time_Relative_hmsf"].astype(str).str.strip(), errors="coerce")
    b["start_time_new"] = (b["start_time_dt"] + b["time_relative_td"]).dt.strftime("%I:%M:%S %p")
    b["_seconds"] = _parse_hms_to_seconds(b["Time_Relative_hms"])
    b["_track"] = b["Behavior"].apply(_classify_behavior)
    activity = b[b["_track"].eq("activity")].copy()
    posture = b[b["_track"].eq("posture")].copy()
    baseline_rows.append(_stage_metrics("after_track_split", b))

    activity_exp = _expand_track_to_seconds(activity)
    posture_exp = _expand_track_to_seconds(posture)
    activity_subset = activity_exp[
        ["Observation", "_second", "rel_time", "Behavior", "Modifier_1", "Modifier_2", "Modifier_3", "start_time_new", "id", "do"]
    ].rename(
        columns={
            "Behavior": "Behavior_activity",
            "Modifier_1": "Modifier_1_activity",
            "Modifier_2": "Modifier_2_activity",
        }
    )
    posture_subset = posture_exp[["Observation", "_second", "Behavior", "Modifier_2"]].rename(
        columns={"Behavior": "Behavior_posture", "Modifier_2": "Modifier_2_posture"}
    )
    merged = activity_subset.merge(
        posture_subset, on=["Observation", "_second"], how="outer", suffixes=("", "_posture")
    ).sort_values(["Observation", "_second"], kind="mergesort")
    for c in ["id", "do", "start_time_new"]:
        merged[c] = merged.groupby("Observation")[c].ffill().bfill()
    merged["rel_time"] = merged["_second"].astype(int).apply(_format_hms)
    merged["Modifier_2"] = merged["Modifier_2_posture"].fillna(merged["Modifier_2_activity"])
    merged["id"] = merged["id"].astype("Int64")
    merged["obs"] = merged["do"].astype("Int64")
    baseline_rows.append(_stage_metrics("after_stream_merge", merged))

    joined = merged.merge(log2[["id", "do", "date"]].rename(columns={"do": "obs"}), on=["id", "obs"], how="left")
    joined["date_time"] = np.where(
        joined["start_time_new"].notna(),
        joined["date"].astype(str).str.strip() + " " + joined["start_time_new"].astype(str).str.strip(),
        np.nan,
    )
    baseline_rows.append(_stage_metrics("joined_pre_steps", joined))

    steps2 = steps.rename(columns={"ID": "id", "Session": "obs", "relative_time_steps": "rel_time"}).copy()
    for c in ["id", "obs", "rel_time"]:
        joined[c] = joined[c].astype(str).str.strip().map(_standardize_rel_time if c == "rel_time" else lambda x: x)
        steps2[c] = steps2[c].astype(str).str.strip().map(_standardize_rel_time if c == "rel_time" else lambda x: x)
    act_wstep = joined.merge(steps2[["id", "obs", "rel_time", "Quality", "Step"]], on=["id", "obs", "rel_time"], how="left")
    baseline_rows.append(_stage_metrics("after_steps_merge", act_wstep))

    # Save outputs
    pd.DataFrame(baseline_rows).to_csv(OUT_DIR / "act24_stage_metrics.csv", index=False)

    # Key-multiplicity diagnostics around the steps merge
    joined_k = joined[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).value_counts()
    steps_k = steps2[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).value_counts()
    merged_k = act_wstep[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).value_counts()
    key_diag = pd.DataFrame(
        [
            {"table": "joined_pre_steps", "total_rows": int(len(joined)), "duplicate_keys": int((joined_k > 1).sum()), "max_multiplicity": int(joined_k.max())},
            {"table": "steps_source", "total_rows": int(len(steps2)), "duplicate_keys": int((steps_k > 1).sum()), "max_multiplicity": int(steps_k.max())},
            {"table": "after_steps_merge", "total_rows": int(len(act_wstep)), "duplicate_keys": int((merged_k > 1).sum()), "max_multiplicity": int(merged_k.max())},
        ]
    )
    key_diag.to_csv(OUT_DIR / "act24_steps_key_multiplicity.csv", index=False)

    steps_dup = (
        steps_k[steps_k > 1]
        .rename("n")
        .reset_index()
        .rename(columns={"index": "id_obs_rel_time"})
        .sort_values("n", ascending=False)
    )
    steps_dup.to_csv(OUT_DIR / "act24_steps_duplicate_keys.csv", index=False)

    dup = (
        act_clean.assign(_k=act_clean[["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1)["_k"])
        if False
        else None
    )
    vc = act_clean[["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1).value_counts()
    top_keys = vc[vc > 1].head(200).rename("n").reset_index().rename(columns={"index": "key"})
    top_keys.to_csv(OUT_DIR / "act24_top_duplicate_datetime_keys.csv", index=False)

    post = act_clean["posture_wbm"].astype(str).str.strip()
    inten = act_clean["intensity_do"].astype(str).str.strip()
    miss = (post != "") & (~post.str.lower().isin(["nan"])) & (
        inten.eq("") | inten.str.lower().isin(["nan", "none", "<na>"])
    )
    miss_df = (
        act_clean.loc[miss, ["id", "obs", "activity_type", "posture_wbm"]]
        .groupby(["id", "obs", "activity_type", "posture_wbm"])
        .size()
        .rename("rows")
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    miss_df.to_csv(OUT_DIR / "act24_missing_intensity_breakdown.csv", index=False)

    print("Wrote:")
    print("-", OUT_DIR / "act24_stage_metrics.csv")
    print("-", OUT_DIR / "act24_top_duplicate_datetime_keys.csv")
    print("-", OUT_DIR / "act24_missing_intensity_breakdown.csv")
    print("-", OUT_DIR / "act24_steps_key_multiplicity.csv")
    print("-", OUT_DIR / "act24_steps_duplicate_keys.csv")


if __name__ == "__main__":
    main()
