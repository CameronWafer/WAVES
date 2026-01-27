import pandas as pd
import numpy as np
from pathlib import Path


DATA_PATH = Path("C:/Users/HELIOS-300/Desktop/Data/am_behposture_onesheet.xlsx")


def print_section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def safe_value_counts(series: pd.Series, top_n: int = 15) -> None:
    vc = series.value_counts(dropna=False).head(top_n)
    print(vc.to_string())


def main() -> None:
    print_section("LOAD")
    print(f"Reading: {DATA_PATH}")
    df = pd.read_excel(DATA_PATH, engine="openpyxl")
    print(f"Rows: {len(df):,}")
    print(f"Cols: {len(df.columns):,}")

    print_section("COLUMNS + DTYPES")
    dtypes = df.dtypes.astype(str)
    print(dtypes.to_string())

    print_section("MISSING VALUES (TOP 20)")
    missing = df.isna().sum().sort_values(ascending=False).head(20)
    print(missing.to_string())

    print_section("DUPLICATE ROWS")
    print(f"Exact duplicate rows: {df.duplicated().sum():,}")

    print_section("BASIC COLUMN PREVIEWS")
    print("Columns:", ", ".join(df.columns))
    print("\nHead (5):")
    print(df.head(5).to_string(index=False))

    # Common AM behavior columns
    for col in ["Event_Type", "Behavior", "Observation", "Modifier_1", "Modifier_2", "Modifier_3", "Modifier_4"]:
        if col in df.columns:
            print_section(f"VALUE COUNTS: {col}")
            safe_value_counts(df[col])

    print_section("UNIQUE VALUES: Behavior + Modifiers")
    for col in ["Behavior", "Modifier_1", "Modifier_2", "Modifier_3", "Modifier_4"]:
        if col in df.columns:
            uniques = df[col].dropna().astype(str).unique()
            uniques = sorted(uniques)
            print(f"{col} unique (non-null) count: {len(uniques)}")
            print(uniques)

    print_section("STATE START FILTER")
    if "Event_Type" in df.columns:
        state_df = df[df["Event_Type"] == "State start"].copy()
        print(f"State start rows: {len(state_df):,} ({len(state_df) / len(df):.2%})")
    else:
        state_df = df
        print("Event_Type column not found; using full dataset.")

    # Try to parse time columns
    print_section("TIME COLUMNS PARSING CHECK")
    time_cols = [c for c in df.columns if "Time" in c or "Date" in c]
    print("Time/Date columns:", ", ".join(time_cols))

    abs_col = "Date_Time_Absolute_dmy_hmsf"
    if abs_col in df.columns:
        abs_dt = pd.to_datetime(df[abs_col], errors="coerce")
        print(f"{abs_col} parsed: {abs_dt.notna().mean():.2%} non-null")
        print(f"Absolute time min: {abs_dt.min()} max: {abs_dt.max()}")

    if "Time_Relative_sf" in df.columns:
        rel = pd.to_numeric(df["Time_Relative_sf"], errors="coerce")
        print(f"Time_Relative_sf parsed: {rel.notna().mean():.2%} non-null")
        print(f"Relative min: {rel.min()} max: {rel.max()}")
        neg = rel < 0
        print(f"Negative Time_Relative_sf rows: {int(neg.sum()):,}")
        if neg.any():
            print("Worst negatives (min 10):")
            print(df.loc[neg, ["Observation", "Time_Relative_sf"]].head(10).to_string(index=False))

    if "Duration_sf" in df.columns:
        dur = pd.to_numeric(df["Duration_sf"], errors="coerce")
        print_section("DURATION_sf SUMMARY")
        print(dur.describe(percentiles=[0.05, 0.5, 0.95]).to_string())
        print(f"Duration <= 0: {int((dur <= 0).sum()):,}")
        print(f"Duration missing: {int(dur.isna().sum()):,}")

    # Observation-level summaries
    if "Observation" in df.columns:
        print_section("OBSERVATION SUMMARY")
        obs_counts = df["Observation"].value_counts()
        print(f"Unique Observation: {obs_counts.size}")
        print("Top 15 Observations by row count:")
        print(obs_counts.head(15).to_string())

        # Optional extraction of AM id and DO label
        obs = df["Observation"].astype(str)
        obs_id = obs.str.extract(r"AM(\d{2})", expand=False)
        do_label = obs.str.extract(r"(DO\d+(?:_[ab])?)", expand=False)
        print("\nExtracted AM id (unique):", sorted(obs_id.dropna().unique())[:15])
        print("Extracted DO label (unique):", sorted(do_label.dropna().unique()))

        copy_mask = obs.str.contains("copyA|copyB", na=False)
        print(f"Rows with copyA/copyB in Observation: {int(copy_mask.sum()):,}")

    # Observation-level span vs. duration sum for State start rows
    if abs_col in df.columns and "Duration_sf" in df.columns and "Observation" in df.columns:
        print_section("OBSERVATION SPAN VS DURATION (STATE START)")
        state_df = state_df.copy()
        state_df[abs_col] = pd.to_datetime(state_df[abs_col], errors="coerce")
        state_df["_dur_s"] = pd.to_numeric(state_df["Duration_sf"], errors="coerce")

        span = (
            state_df.dropna(subset=[abs_col])
            .groupby("Observation")[abs_col]
            .agg(min_dt="min", max_dt="max")
            .reset_index()
        )
        span_seconds = (span["max_dt"] - span["min_dt"]).dt.total_seconds()
        span["span_s_inclusive"] = pd.to_numeric(span_seconds, errors="coerce") + 1

        dur_sum = (
            state_df.groupby("Observation")["_dur_s"]
            .sum(min_count=1)
            .reset_index(name="duration_sum")
        )

        merged = span.merge(dur_sum, on="Observation", how="left")
        merged["span_hours"] = merged["span_s_inclusive"] / 3600.0
        merged = merged.sort_values("span_s_inclusive", ascending=False)
        print("Top 10 longest spans:")
        print(merged.head(10).to_string(index=False))

        # Compare total duration vs span (for overlap / gaps insights)
        merged["duration_ratio"] = merged["duration_sum"] / merged["span_s_inclusive"]
        print("\nLowest duration/span ratios (top 10):")
        print(
            merged.sort_values("duration_ratio", ascending=True)
            .head(10)
            .to_string(index=False)
        )

    # Estimate potential second-by-second size for state starts
    if "Duration_sf" in df.columns and "Observation" in df.columns:
        print_section("SECOND-BY-SECOND SIZE ESTIMATE (STATE START)")
        state_df = state_df.copy()
        state_df["_dur_s"] = pd.to_numeric(state_df["Duration_sf"], errors="coerce").fillna(0.0)
        state_df["_dur_s_ceil"] = np.ceil(state_df["_dur_s"]).astype("int64")
        est_rows = state_df.groupby("Observation")["_dur_s_ceil"].sum()
        print(f"Estimated total seconds (sum of ceil durations): {int(est_rows.sum()):,}")
        print("Top 10 observations by estimated seconds:")
        print(est_rows.sort_values(ascending=False).head(10).to_string())

    # Build second-by-second grid and validate coverage (matches notebook logic)
    if abs_col in df.columns and "Duration_sf" in df.columns and "Observation" in df.columns:
        print_section("SEC-BY-SEC BUILD + INTEGRITY CHECKS")
        work = state_df.copy()
        work[abs_col] = pd.to_datetime(work[abs_col], errors="coerce")
        work = work.dropna(subset=["Observation", abs_col]).copy()

        work["_start_dt_sec"] = work[abs_col].dt.floor("s")
        work["_dur_s"] = pd.to_numeric(work["Duration_sf"], errors="coerce").fillna(0.0)
        work["_dur_s_int"] = np.ceil(work["_dur_s"]).astype("int64")
        work["_end_dt_sec"] = work["_start_dt_sec"] + pd.to_timedelta(work["_dur_s_int"], unit="s")

        # stable ordering for "latest start wins"
        work = work.sort_values(["Observation", "_start_dt_sec", abs_col], kind="mergesort")

        helper_cols = {"_start_dt_sec", "_dur_s", "_dur_s_int", "_end_dt_sec"}
        carry_cols = [c for c in work.columns if c not in helper_cols]

        out = []
        for obs, g in work.groupby("Observation", sort=False):
            start_dt = g["_start_dt_sec"].min()
            end_dt = g["_end_dt_sec"].max()
            grid = pd.date_range(start=start_dt, end=end_dt, freq="1s")

            starts = g["_start_dt_sec"].to_numpy()
            ends = g["_end_dt_sec"].to_numpy()
            tvals = grid.to_numpy()
            idx = np.searchsorted(starts, tvals, side="right") - 1

            res = pd.DataFrame({"Observation": obs, "date_time_abs": grid})
            res["_sec"] = (res["date_time_abs"] - start_dt).dt.total_seconds().astype("int64")

            valid = idx >= 0
            valid &= (tvals <= ends[np.maximum(idx, 0)])
            if valid.any():
                take = g.iloc[idx[valid]][carry_cols].reset_index(drop=True)
                for c in take.columns:
                    if c in {"Observation"}:
                        continue
                    res.loc[valid, c] = take[c].to_numpy()

            out.append(res)

        sec_by_sec = pd.concat(out, ignore_index=True)

        print(f"sec_by_sec rows: {len(sec_by_sec):,}")
        print(f"Unique Observation (sec): {sec_by_sec['Observation'].nunique()}")

        # 1) no duplicate seconds per Observation
        dup = sec_by_sec.duplicated(subset=["Observation", "date_time_abs"]).sum()
        print(f"Duplicate (Observation, date_time_abs): {dup}")

        # 2) contiguity
        def _is_contig(g: pd.DataFrame) -> bool:
            dt = g["date_time_abs"].sort_values().to_numpy()
            if len(dt) <= 1:
                return True
            diffs = np.diff(dt).astype("timedelta64[s]").astype(int)
            return np.all(diffs == 1)

        contig = sec_by_sec.groupby("Observation", sort=False).apply(_is_contig)
        bad_contig = contig[~contig]
        print(f"Non-contiguous Observations: {len(bad_contig)}")
        if len(bad_contig) > 0:
            print("Examples:", bad_contig.index[:10].tolist())

        # 3) coverage rate: % seconds with a Behavior value
        if "Behavior" in sec_by_sec.columns:
            cov = sec_by_sec.groupby("Observation", sort=False).apply(
                lambda g: float(g["Behavior"].notna().mean())
            )
            print(
                "Coverage rate per observation (min/median/max):",
                float(cov.min()),
                float(cov.median()),
                float(cov.max()),
            )
            worst = cov.sort_values().head(10)
            print("Worst coverage observations (lowest 10):")
            print(worst.to_string())

        # 4) event-to-grid consistency: for each obs, grid length vs span
        span = (
            sec_by_sec.groupby("Observation", sort=False)["date_time_abs"]
            .agg(min_dt="min", max_dt="max", n="count")
            .reset_index()
        )
        span["span_s_inclusive"] = (
            (span["max_dt"] - span["min_dt"]).dt.total_seconds() + 1
        )
        span["n_minus_span"] = span["n"] - span["span_s_inclusive"]
        print("Grid length minus span (should be 0):")
        print(span["n_minus_span"].describe().to_string())

        # 5) forward-fill Behavior + Modifiers per Observation (expected in sec-by-sec)
        print_section("SEC-BY-SEC WITH CARRY-FORWARD (Behavior + Modifiers)")
        carry_cols = ["Behavior", "Modifier_1", "Modifier_2", "Modifier_3", "Modifier_4"]
        available = [c for c in carry_cols if c in sec_by_sec.columns]
        if available:
            sec_by_sec_cf = sec_by_sec.sort_values(
                ["Observation", "date_time_abs"], kind="mergesort"
            ).copy()
            sec_by_sec_cf[available] = sec_by_sec_cf.groupby("Observation", sort=False)[
                available
            ].ffill()

            # After carry-forward, report any remaining NaNs in these columns
            print("Remaining NaNs after forward-fill:")
            remaining = sec_by_sec_cf[available].isna().sum()
            print(remaining.to_string())

            # Show one short sample where carry-forward changed values
            changed = sec_by_sec_cf[available].isna().sum().sum() == 0
            print(f"All carry-forward columns fully filled: {bool(changed)}")
        else:
            print("No carry-forward columns available in sec_by_sec.")

    print_section("DONE")
    print("Profiling complete.")


if __name__ == "__main__":
    main()
