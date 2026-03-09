from pathlib import Path
import pandas as pd


ACT_DIR = Path("C:/Users/HELIOS-300/Desktop/WAVES/ACT24 Full Code")

PATHS = {
    "clean": ACT_DIR / "Cameron_ACT24_Clean.csv",
    "clean_nodrop": ACT_DIR / "Cameron_ACT24_Clean_NoDrop.csv",
    "waves": ACT_DIR / "Cameron_ACT24_Clean_WavesReady.csv",
    "waves_nodrop": ACT_DIR / "Cameron_ACT24_Clean_WavesReady_NoDrop.csv",
}


def _is_missing_text(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin(["", "nan", "none", "<na>"])


def _safe_vc_key(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    base = df[cols].astype(str).agg("|".join, axis=1)
    return base.value_counts()


def _print_file_audit(name: str, df: pd.DataFrame) -> None:
    print(f"\n=== {name} ===")
    print("rows:", len(df))
    print("cols:", len(df.columns))

    if {"id", "obs", "rel_time"}.issubset(df.columns):
        vc = _safe_vc_key(df, ["id", "obs", "rel_time"])
        print("dup id+obs+rel_time keys:", int((vc > 1).sum()))
        print("dup id+obs+rel_time max multiplicity:", int(vc.max()) if len(vc) else 0)
    else:
        print("dup id+obs+rel_time keys: n/a")

    if {"id", "obs", "date_time"}.issubset(df.columns):
        dt = df[~_is_missing_text(df["date_time"])]
        if len(dt) > 0:
            vc_dt = _safe_vc_key(dt, ["id", "obs", "date_time"])
            print("dup id+obs+date_time keys:", int((vc_dt > 1).sum()))
            print("dup id+obs+date_time max multiplicity:", int(vc_dt.max()) if len(vc_dt) else 0)
        else:
            print("dup id+obs+date_time keys: 0 (no valid date_time rows)")
    else:
        print("dup id+obs+date_time keys: n/a")

    if {"id", "obs", "rel_time"}.issubset(df.columns):
        rel_td = pd.to_timedelta(df["rel_time"].astype(str).str.strip(), errors="coerce")
        print("rel_time unparsable rows:", int(rel_td.isna().sum()))
        print("rel_time negative rows:", int((rel_td < pd.Timedelta(0)).sum()))
        sess_max = (
            df.assign(_rel_td=rel_td)
            .dropna(subset=["_rel_td"])
            .groupby(["id", "obs"], dropna=False)["_rel_td"]
            .max()
            .reset_index(name="max_rel_td")
        )
        over_2h = sess_max[sess_max["max_rel_td"] > pd.Timedelta(hours=2)].copy()
        print("sessions with max rel_time > 2h:", len(over_2h))
        if len(over_2h) > 0:
            show = over_2h.sort_values("max_rel_td", ascending=False).head(15).copy()
            show["max_rel_time"] = show["max_rel_td"].astype(str)
            print(show[["id", "obs", "max_rel_time"]].to_string(index=False))
    else:
        print("sessions with max rel_time > 2h: n/a")

    if {"posture_wbm", "intensity_do"}.issubset(df.columns):
        post = df["posture_wbm"].astype(str).str.strip().str.lower()
        inten = df["intensity_do"].astype(str).str.strip().str.lower()
        miss = (~_is_missing_text(df["posture_wbm"])) & (_is_missing_text(df["intensity_do"]))
        print("missing intensity with posture present:", int(miss.sum()))
        if int(miss.sum()) > 0:
            print(df.loc[miss, "posture_wbm"].value_counts().head(10).to_string())

    if "Step" in df.columns:
        s = df["Step"].astype(str).str.strip()
        na_txt = s.eq("NA").sum()
        blank_like = s.str.lower().isin(["", "nan", "none", "<na>"]).sum()
        num = pd.to_numeric(s, errors="coerce")
        print("Step == 'NA' rows:", int(na_txt))
        print("Step blank-like rows:", int(blank_like))
        print("Step numeric rows:", int(num.notna().sum()))

    if "steps_do" in df.columns:
        s = df["steps_do"].astype(str).str.strip()
        na_txt = s.eq("NA").sum()
        blank_like = s.str.lower().isin(["", "nan", "none", "<na>"]).sum()
        num = pd.to_numeric(s, errors="coerce")
        print("steps_do == 'NA' rows:", int(na_txt))
        print("steps_do blank-like rows:", int(blank_like))
        print("steps_do numeric rows:", int(num.notna().sum()))


def _compare_pairs(name_a: str, df_a: pd.DataFrame, name_b: str, df_b: pd.DataFrame) -> None:
    print(f"\n=== Compare: {name_a} vs {name_b} ===")
    print(f"{name_a} rows:", len(df_a))
    print(f"{name_b} rows:", len(df_b))
    print("row diff (b-a):", len(df_b) - len(df_a))

    if {"id", "obs", "rel_time"}.issubset(df_a.columns) and {"id", "obs", "rel_time"}.issubset(df_b.columns):
        a_key = set(df_a[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).tolist())
        b_key = set(df_b[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).tolist())
        print("keys only in A (id,obs,rel_time):", len(a_key - b_key))
        print("keys only in B (id,obs,rel_time):", len(b_key - a_key))
    elif {"id", "obs", "date_time"}.issubset(df_a.columns) and {"id", "obs", "date_time"}.issubset(df_b.columns):
        a_key = set(df_a[["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1).tolist())
        b_key = set(df_b[["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1).tolist())
        print("keys only in A (id,obs,date_time):", len(a_key - b_key))
        print("keys only in B (id,obs,date_time):", len(b_key - a_key))
    else:
        print("key comparison skipped (schema mismatch)")


def main() -> None:
    existing: dict[str, pd.DataFrame] = {}
    print("=== File presence ===")
    for name, path in PATHS.items():
        print(f"{name}: {'FOUND' if path.exists() else 'MISSING'} -> {path}")
        if path.exists():
            existing[name] = pd.read_csv(path, dtype=str, keep_default_na=False)

    for name, df in existing.items():
        _print_file_audit(name, df)

    if "clean" in existing and "clean_nodrop" in existing:
        _compare_pairs("clean", existing["clean"], "clean_nodrop", existing["clean_nodrop"])
    else:
        print("\n=== Compare: clean vs clean_nodrop ===")
        print("Skipped: one or both files are missing.")

    if "waves" in existing and "waves_nodrop" in existing:
        _compare_pairs("waves", existing["waves"], "waves_nodrop", existing["waves_nodrop"])
    else:
        print("\n=== Compare: waves vs waves_nodrop ===")
        print("Skipped: one or both files are missing.")

    if "clean_nodrop" in existing and "waves_nodrop" in existing:
        print("\n=== Cross-check: clean_nodrop vs waves_nodrop ===")
        c = existing["clean_nodrop"].copy()
        w = existing["waves_nodrop"].copy()
        if {"pid", "observation"}.issubset(w.columns):
            w = w.rename(columns={"pid": "id", "observation": "obs"})
        if {"id", "obs", "date_time"}.issubset(c.columns) and {"id", "obs", "date_time"}.issubset(w.columns):
            ck = set(c[["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1).tolist())
            wk = set(w[["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1).tolist())
            print("date_time keys only in clean_nodrop:", len(ck - wk))
            print("date_time keys only in waves_nodrop:", len(wk - ck))
        else:
            print("date_time key cross-check skipped (missing columns)")


if __name__ == "__main__":
    main()
