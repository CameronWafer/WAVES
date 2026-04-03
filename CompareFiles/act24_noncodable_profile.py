from pathlib import Path
import pandas as pd


ACT_DIR = Path("C:/Users/HELIOS-300/Desktop/WAVES/ACT24 Full Code")
OUT_DIR = Path("C:/Users/HELIOS-300/Desktop/WAVES/CompareFiles/act24_diag")

FILES = {
    "clean": ACT_DIR / "Cameron_ACT24_Clean.csv",
    "clean_nodrop": ACT_DIR / "Cameron_ACT24_Clean_NoDrop.csv",
}


def _load(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()


def _is_missing_text(s: pd.Series) -> pd.Series:
    return _norm(s).isin(["", "nan", "none", "<na>"])


def _print_top(df: pd.DataFrame, col: str, n: int = 20) -> None:
    if col not in df.columns:
        print(f"{col}: [column missing]")
        return
    print(f"\nTop {n} values: {col}")
    vc = df[col].value_counts(dropna=False).head(n)
    if len(vc) == 0:
        print("[no rows]")
    else:
        print(vc.to_string())


def _time_profile(df: pd.DataFrame) -> pd.DataFrame:
    if not {"id", "obs", "rel_time", "date_time"}.issubset(df.columns):
        return pd.DataFrame()
    d = df.copy()
    d["_rel_td"] = pd.to_timedelta(d["rel_time"].astype(str).str.strip(), errors="coerce")
    d["_dt"] = pd.to_datetime(d["date_time"], errors="coerce")
    g = (
        d.groupby(["id", "obs"], dropna=False)
        .agg(
            rows=("id", "size"),
            rel_min=("rel_time", "min"),
            rel_max=("rel_time", "max"),
            rel_td_min=("_rel_td", "min"),
            rel_td_max=("_rel_td", "max"),
            dt_min=("_dt", "min"),
            dt_max=("_dt", "max"),
        )
        .reset_index()
    )
    g["rel_span_s"] = (g["rel_td_max"] - g["rel_td_min"]).dt.total_seconds()
    g["dt_span_s"] = (g["dt_max"] - g["dt_min"]).dt.total_seconds()
    return g.sort_values(["rows", "id", "obs"], ascending=[False, True, True])


def profile_file(name: str, df: pd.DataFrame) -> None:
    print(f"\n==================== {name} ====================")
    print("rows:", len(df))
    if "activity_type" not in df.columns:
        print("No activity_type column; skipping.")
        return

    act = _norm(df["activity_type"])
    noncod = df.loc[act.eq("non_codable")].copy()
    print("non_codable rows:", len(noncod))
    print("non_codable %:", round((len(noncod) / len(df) * 100.0), 3) if len(df) else 0.0)

    if len(noncod) == 0:
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_rows = OUT_DIR / f"act24_{name}_non_codable_rows.csv"
    noncod.to_csv(out_rows, index=False)
    print("saved rows to:", out_rows)

    # Missingness profile for all columns
    miss = []
    for c in noncod.columns:
        m = int(_is_missing_text(noncod[c]).sum())
        miss.append((c, m, round(m / len(noncod) * 100.0, 3)))
    miss_df = pd.DataFrame(miss, columns=["column", "missing_rows", "missing_pct"]).sort_values(
        ["missing_rows", "column"], ascending=[False, True]
    )
    out_miss = OUT_DIR / f"act24_{name}_non_codable_missingness.csv"
    miss_df.to_csv(out_miss, index=False)
    print("saved missingness to:", out_miss)

    _print_top(noncod, "id")
    _print_top(noncod, "obs")
    _print_top(noncod, "posture_wbm")
    _print_top(noncod, "broad_domain")
    _print_top(noncod, "broad.behavior_do")
    _print_top(noncod, "intensity_do")
    _print_top(noncod, "Quality")
    _print_top(noncod, "Step")

    # Key session/time summary
    sess = _time_profile(noncod)
    if len(sess) > 0:
        out_sess = OUT_DIR / f"act24_{name}_non_codable_session_time_profile.csv"
        sess.to_csv(out_sess, index=False)
        print("\nnon_codable unique id/obs sessions:", sess.shape[0])
        print("saved session/time profile to:", out_sess)
        print("\nTop sessions by non_codable row count:")
        print(sess[["id", "obs", "rows", "rel_min", "rel_max"]].head(20).to_string(index=False))

    # Show a compact sample of full rows
    sample_cols = [c for c in ["id", "obs", "date_time", "rel_time", "activity_type", "posture_wbm", "intensity_do", "Quality", "Step"] if c in noncod.columns]
    print("\nSample non_codable rows (first 20 sorted by id/obs/rel_time):")
    s = noncod.copy()
    if "rel_time" in s.columns:
        s["_rel_td"] = pd.to_timedelta(s["rel_time"].astype(str).str.strip(), errors="coerce")
        s = s.sort_values(["id", "obs", "_rel_td", "date_time"], kind="mergesort")
    else:
        s = s.sort_values(["id", "obs"], kind="mergesort")
    print(s[sample_cols].head(20).to_string(index=False))


def main() -> None:
    for name, path in FILES.items():
        print(f"\nFile: {name} -> {path}")
        if not path.exists():
            print("MISSING")
            continue
        df = _load(path)
        profile_file(name, df)


if __name__ == "__main__":
    main()
