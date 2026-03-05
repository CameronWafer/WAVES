import pandas as pd


clean_path = "C:/Users/HELIOS-300/Desktop/WAVES/ACT24 Full Code/Cameron_ACT24_Clean.csv"
waves_path = "C:/Users/HELIOS-300/Desktop/WAVES/ACT24 Full Code/Cameron_ACT24_Clean_WavesReady.csv"

df = pd.read_csv(clean_path, dtype=str, keep_default_na=False)
wf = pd.read_csv(waves_path, dtype=str, keep_default_na=False)

print("clean_rows", len(df))
print("waves_rows", len(wf))
print("row_count_match", len(df) == len(wf))

id_clean = pd.to_numeric(df.get("id", ""), errors="coerce")
id_wf = pd.to_numeric(wf.get("pid", ""), errors="coerce")
print("id135_in_clean", int((id_clean == 135).sum()))
print("id135_in_wavesready_pid", int((id_wf == 135).sum()))

k = df[["id", "obs", "rel_time"]].astype(str).agg("|".join, axis=1).value_counts()
print("dup_id_obs_rel_time_keys", int((k > 1).sum()))
print("dup_id_obs_rel_time_max_mult", int(k.max()) if len(k) else 0)

if "date_time" in df.columns:
    dt = df[df["date_time"].astype(str).str.strip().str.lower().ne("nan")]
    kd = dt[["id", "obs", "date_time"]].astype(str).agg("|".join, axis=1).value_counts()
    print("dup_id_obs_date_time_keys", int((kd > 1).sum()))
    print("dup_id_obs_date_time_max_mult", int(kd.max()) if len(kd) else 0)

post = df["posture_wbm"].astype(str).str.strip().str.lower()
inten = df["intensity_do"].astype(str).str.strip().str.lower()
miss = (post != "") & (~post.isin(["nan", "none", "<na>"])) & (inten.isin(["", "nan", "none", "<na>"]))
print("missing_intensity_with_posture", int(miss.sum()))

print("unique_ids_clean", df["id"].nunique())
print("unique_id_obs_pairs", df[["id", "obs"]].drop_duplicates().shape[0])

for col in ["steps_do", "posture_do", "intensity3_do", "intensity4_do"]:
    if col in wf.columns:
        missing = wf[col].astype(str).str.strip().str.lower().isin(["", "nan", "none", "<na>"]).sum()
        print(f"waves_missing_{col}", int(missing))
    else:
        print(f"waves_missing_{col}", "column_not_found")
