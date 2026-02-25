import pandas as pd
import numpy as np
from pathlib import Path


BEHAV_PATH = "C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx"
LOG_PATH = "C:/Users/HELIOS-300/Desktop/Data/DO_LOG_final.csv"
OUT_DIR = Path("C:/Users/HELIOS-300/Desktop/WAVES/CompareFiles")


def fmt_secs(v) -> str:
    if pd.isna(v):
        return "blank"
    v = float(v)
    sign = "-" if v < 0 else ""
    vv = abs(v)
    h = int(vv // 3600)
    m = int((vv % 3600) // 60)
    s = int(round(vv % 60))
    return f"{sign}{h:02d}:{m:02d}:{s:02d} ({v:.1f}s)"


def fmt_group_label(id_num, do_base) -> str:
    if pd.isna(id_num) or pd.isna(do_base):
        return "Unknown group"
    return f"AM{int(id_num):02d} {do_base}"


def load_behav() -> pd.DataFrame:
    b = pd.read_excel(BEHAV_PATH, engine="openpyxl")
    b = b[b["Event_Type"].eq("State start")].copy()
    b["id_num"] = pd.to_numeric(
        b["Observation"].astype(str).str.extract(r"AM(\d{2})", expand=False), errors="coerce"
    )
    b["do_base"] = b["Observation"].astype(str).str.extract(r"(DO\d+)", expand=False)
    b["copy_tag"] = b["Observation"].astype(str).str.extract(r"(copyA|copyB)", expand=False)
    b["abs_dt"] = pd.to_datetime(b["Date_Time_Absolute_dmy_hmsf"], errors="coerce")
    rel_hmsf = pd.to_timedelta(
        b["Time_Relative_hmsf"].astype(str).str.strip(), errors="coerce"
    ).dt.total_seconds()
    rel_hms = pd.to_timedelta(
        b["Time_Relative_hms"].astype(str).str.strip(), errors="coerce"
    ).dt.total_seconds()
    b["rel_raw_s"] = rel_hmsf.fillna(rel_hms)
    b["dur_s"] = np.ceil(pd.to_numeric(b["Duration_sf"], errors="coerce").fillna(0.0))
    b["rel_end_s"] = b["rel_raw_s"] + b["dur_s"]
    return b


def load_log() -> pd.DataFrame:
    l = pd.read_csv(LOG_PATH, encoding="utf-8")
    l["id_num"] = pd.to_numeric(
        l["id"].astype(str).str.extract(r"AM(\d{2})", expand=False), errors="coerce"
    )
    l["do"] = l["obs"].astype(str).str.strip()
    l["do_base"] = l["do"].str.replace(r"_(a|b)$", "", regex=True)
    l["start_date"] = pd.to_datetime(
        dict(year=l["start_year"], month=l["start_month"], day=l["start_day"]),
        errors="coerce",
    )
    l["start_dt"] = pd.to_datetime(
        l["start_date"].dt.strftime("%Y-%m-%d") + " " + l["start_time"].astype(str),
        errors="coerce",
    )
    stop_td = pd.to_timedelta(l["stop_time"].astype(str).str.strip(), errors="coerce")
    l["stop_dt"] = l["start_date"] + stop_td
    overnight = l["stop_dt"] < l["start_dt"]
    l.loc[overnight, "stop_dt"] = l.loc[overnight, "stop_dt"] + pd.Timedelta(days=1)
    l["dur_log_s"] = (l["stop_dt"] - l["start_dt"]).dt.total_seconds()
    return l


def summarize_observations(beh: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for obs, g in beh.groupby("Observation", sort=False):
        g2 = g.dropna(subset=["rel_raw_s", "rel_end_s"]).sort_values("rel_raw_s")
        if g2.empty:
            union_cov = np.nan
            rel_min = np.nan
            rel_max = np.nan
        else:
            arr = g2[["rel_raw_s", "rel_end_s"]].to_numpy(dtype=float)
            cur_s, cur_e = arr[0]
            union_cov = 0.0
            for s, e in arr[1:]:
                if s <= cur_e:
                    cur_e = max(cur_e, e)
                else:
                    union_cov += cur_e - cur_s
                    cur_s, cur_e = s, e
            union_cov += cur_e - cur_s
            rel_min = float(g2["rel_raw_s"].min())
            rel_max = float(g2["rel_end_s"].max())

        rows.append(
            {
                "Observation": obs,
                "id_num": g["id_num"].iloc[0],
                "do_base": g["do_base"].iloc[0],
                "copy_tag": g["copy_tag"].iloc[0],
                "rows": len(g),
                "abs_start": g["abs_dt"].min(),
                "abs_end": g["abs_dt"].max(),
                "rel_min_raw_s": rel_min,
                "rel_max_raw_s": rel_max,
                "negative_raw_seconds": max(0.0, -rel_min) if pd.notna(rel_min) else np.nan,
                "positive_raw_seconds": max(0.0, rel_max) if pd.notna(rel_max) else np.nan,
                "union_coverage_seconds": union_cov,
                "raw_rel_neg_rows": int((g["rel_raw_s"] < 0).sum()),
                "raw_rel_missing_rows": int(g["rel_raw_s"].isna().sum()),
                "abs_dt_missing_rows": int(g["abs_dt"].isna().sum()),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    beh = load_behav()
    log = load_log()
    obs = summarize_observations(beh)

    # 1) Missing session coverage in log
    beh_groups = obs[["id_num", "do_base"]].drop_duplicates()
    log_groups = log[["id_num", "do_base"]].drop_duplicates()
    missing_in_log = beh_groups.merge(log_groups, on=["id_num", "do_base"], how="left", indicator=True)
    missing_in_log = missing_in_log[missing_in_log["_merge"].eq("left_only")].drop(columns="_merge")

    # 2) Log split structure
    log_group_summary = (
        log.groupby(["id_num", "do_base"], as_index=False)
        .agg(
            log_rows=("do", "size"),
            split_rows=("do", lambda s: int(s.str.contains("_[ab]$", regex=True).sum())),
            unsuffixed_rows=("do", lambda s: int((~s.str.contains("_[ab]$", regex=True)).sum())),
            log_total_duration_s=("dur_log_s", "sum"),
            log_first_start=("start_dt", "min"),
            log_last_stop=("stop_dt", "max"),
        )
    )

    # 3) Behavior group structure
    beh_group_summary = (
        obs.groupby(["id_num", "do_base"], as_index=False)
        .agg(
            beh_observations=("Observation", "size"),
            beh_copy_observations=("copy_tag", lambda s: int(s.notna().sum())),
            beh_union_coverage_sum_s=("union_coverage_seconds", "sum"),
            beh_negative_raw_sum_s=("negative_raw_seconds", "sum"),
            beh_abs_first=("abs_start", "min"),
            beh_abs_last=("abs_end", "max"),
        )
    )
    beh_group_summary["beh_span_seconds"] = (
        beh_group_summary["beh_abs_last"] - beh_group_summary["beh_abs_first"]
    ).dt.total_seconds()

    # 4) Side-by-side comparison
    side_by_side = beh_group_summary.merge(
        log_group_summary, on=["id_num", "do_base"], how="outer"
    )
    side_by_side["coverage_minus_log_s"] = (
        side_by_side["beh_union_coverage_sum_s"] - side_by_side["log_total_duration_s"]
    )

    # 5) Special flag: groups likely impossible to resolve deterministically
    side_by_side["flag_missing_log_group"] = side_by_side["log_rows"].isna()
    side_by_side["flag_multi_copy_vs_few_log_rows"] = (
        side_by_side["beh_observations"].fillna(0) > side_by_side["log_rows"].fillna(0)
    )
    side_by_side["flag_unsuffixed_multi_log_rows"] = side_by_side["unsuffixed_rows"].fillna(0) > 1
    side_by_side["flag_large_coverage_mismatch"] = side_by_side["coverage_minus_log_s"].abs() > 120
    side_by_side["flag_has_negative_raw_time"] = side_by_side["beh_negative_raw_sum_s"].fillna(0) > 0

    # 6) Observation-level problematic rows
    obs_problematic = obs[
        (obs["raw_rel_neg_rows"] > 0)
        | (obs["raw_rel_missing_rows"] > 0)
        | (obs["abs_dt_missing_rows"] > 0)
    ].copy()

    # Save CSV artifacts
    missing_in_log.to_csv(OUT_DIR / "am_report_missing_log_groups.csv", index=False)
    log_group_summary.to_csv(OUT_DIR / "am_report_log_group_summary.csv", index=False)
    obs.to_csv(OUT_DIR / "am_report_observation_summary.csv", index=False)
    side_by_side.sort_values(["id_num", "do_base"]).to_csv(
        OUT_DIR / "am_report_side_by_side.csv", index=False
    )
    obs_problematic.sort_values(["id_num", "do_base", "Observation"]).to_csv(
        OUT_DIR / "am_report_problematic_observations.csv", index=False
    )

    # Markdown executive report (plain-language for non-technical readers)
    unresolved = side_by_side[
        side_by_side[
            [
                "flag_missing_log_group",
                "flag_multi_copy_vs_few_log_rows",
                "flag_unsuffixed_multi_log_rows",
                "flag_large_coverage_mismatch",
                "flag_has_negative_raw_time",
            ]
        ].any(axis=1)
    ].copy()

    md = []
    md.append("# AM Data Accuracy Report (Plain Language)\n")
    md.append("This report lists data issues that block 100% accurate merging. It avoids assumptions and shows exactly what is missing or ambiguous.\n")

    md.append("\n## Quick totals\n")
    md.append(f"- Total behavior observations checked: {obs['Observation'].nunique():,}\n")
    md.append(f"- Total log session rows: {len(log):,}\n")
    md.append(f"- Session groups flagged for manual decision: {len(unresolved)}\n")

    md.append("\n## 1) Missing in log file\n")
    if len(missing_in_log) == 0:
        md.append("- None.\n")
    else:
        for _, r in missing_in_log.sort_values(["id_num", "do_base"]).iterrows():
            md.append(f"- We are missing `{fmt_group_label(r['id_num'], r['do_base'])}` in `log_df`.\n")

    md.append("\n## 2) AM15 DO2 detailed example\n")
    am15 = obs[(obs["id_num"] == 15) & (obs["do_base"] == "DO2")].copy()
    log15 = log[(log["id_num"] == 15) & (log["do_base"] == "DO2")].copy()
    md.append(f"- Behavior has {len(am15)} observations.\n")
    md.append(f"- Log has {len(log15)} rows.\n")
    if len(am15):
        md.append("- The 4 behavior observations are:\n")
        for _, r in am15.sort_values("Observation").iterrows():
            md.append(
                f"  - `{r['Observation']}`: observed coverage duration {fmt_secs(r['union_coverage_seconds'])}"
                f"; negative relative time offset {fmt_secs(r['negative_raw_seconds'])}\n"
            )
    if len(log15):
        md.append("- Log rows for AM15 DO2 are:\n")
        for _, r in log15.sort_values("start_dt").iterrows():
            md.append(
                f"  - `{r['do']}`: log duration {fmt_secs(r['dur_log_s'])} "
                f"(start {r['start_dt']}, stop {r['stop_dt']})\n"
            )

    md.append("\n## 3) Observations with negative relative time values\n")
    neg_obs = obs_problematic[obs_problematic["raw_rel_neg_rows"] > 0].copy()
    if len(neg_obs) == 0:
        md.append("- None.\n")
    else:
        md.append(f"- Count: {len(neg_obs)} observations.\n")
        for _, r in neg_obs.sort_values(["id_num", "do_base", "Observation"]).iterrows():
            md.append(
                f"- `{r['Observation']}` ({fmt_group_label(r['id_num'], r['do_base'])}): "
                f"{int(r['raw_rel_neg_rows'])} rows with negative relative time; "
                f"largest negative offset {fmt_secs(r['negative_raw_seconds'])}.\n"
            )

    md.append("\n## 4) Groups needing manual decision (non-deterministic)\n")
    md.append("- These groups cannot be resolved 100% accurately without additional rules or source corrections.\n")
    for _, r in unresolved.sort_values(["id_num", "do_base"]).iterrows():
        reasons = []
        if bool(r["flag_missing_log_group"]):
            reasons.append("missing in log")
        if bool(r["flag_multi_copy_vs_few_log_rows"]):
            reasons.append("more behavior observations than log rows")
        if bool(r["flag_unsuffixed_multi_log_rows"]):
            reasons.append("multiple log rows are unsuffixed (no _a/_b)")
        if bool(r["flag_large_coverage_mismatch"]):
            reasons.append(
                f"duration mismatch: behavior {fmt_secs(r['beh_union_coverage_sum_s'])} vs log {fmt_secs(r['log_total_duration_s'])}"
            )
        if bool(r["flag_has_negative_raw_time"]):
            reasons.append(f"negative relative time exists (sum offset {fmt_secs(r['beh_negative_raw_sum_s'])})")

        reason_text = "; ".join(reasons) if reasons else "unspecified issue"
        md.append(f"- `{fmt_group_label(r['id_num'], r['do_base'])}`: {reason_text}.\n")

    md.append("\n## 5) What this means\n")
    md.append("- Any pipeline step that auto-combines these flagged groups will require assumptions.\n")
    md.append("- To stay 100% accurate, each flagged group should be resolved manually (or with new source data).\n")
    md.append("- Use `am_report_side_by_side.csv` and `am_report_observation_summary.csv` for full numeric detail.\n")

    (OUT_DIR / "AM_INTEGRITY_REPORT.md").write_text("\n".join(md), encoding="utf-8")

    print("Wrote report files:")
    print("-", OUT_DIR / "AM_INTEGRITY_REPORT.md")
    print("-", OUT_DIR / "am_report_side_by_side.csv")
    print("-", OUT_DIR / "am_report_missing_log_groups.csv")
    print("-", OUT_DIR / "am_report_problematic_observations.csv")
    print("-", OUT_DIR / "am_report_log_group_summary.csv")
    print("-", OUT_DIR / "am_report_observation_summary.csv")


if __name__ == "__main__":
    main()
