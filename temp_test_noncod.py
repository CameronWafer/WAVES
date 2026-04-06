import pandas as pd

df = pd.read_csv("c:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv")

print("=== COLUMNS ===")
print(df.columns.tolist())

print("\n=== non_codable rows sample (regular file) ===")
nc = df[df["activity_type"] == "non_codable"].head(10)
print(nc[["id", "obs", "rel_time", "activity_type", "broad_domain", "broad.behavior_do",
          "posture_wbm", "posture_broad", "broad.posture_do", "sed.posture_do", "intensity_do"]].to_string())

# ------------------------------------------------------------
# Simulate current WavesReady logic (as-is in notebook)
# ------------------------------------------------------------
wv = df.copy()

domain_map = {
    "leisure": "leisure", "Leisure_Screen": "leisure", "exercise": "leisure",
    "household": "household", "maintenance_repair": "household", "lawn_garden": "household",
    "personal": "household", "Trav_car": "transportation", "active_transportation": "transportation",
    "transportation": "transportation", "work_education": "occupation",
    "purchase_other": "other", "sleep": "other", "non_codable": "other",
}
wv["domain_do"] = wv["broad_domain"].map(domain_map)

posture_do_map = {
    "sedentary": "sedentary", "sed_drive": "sedentary", "stationary": "mixed_movement",
    "mixed_movement": "mixed_movement", "walking": "walking", "running": "running", "cycling": "biking",
}
wv["posture_do"] = wv["broad.posture_do"].map(posture_do_map)

activity_norm = wv["activity_type"].astype("string").str.strip().str.lower()
posture_norm  = wv["posture_wbm"].astype("string").str.strip().str.lower()
vehicle_mask  = activity_norm.isin(["trav_drive", "trav_pass"])
lying_mask    = posture_norm.eq("lying")
sitting_mask  = posture_norm.eq("sitting")

wv["Sedtype_do"] = "non_sedentary"
wv.loc[sitting_mask, "Sedtype_do"] = "sit_lie"
wv.loc[lying_mask,   "Sedtype_do"] = "Lying"
wv.loc[vehicle_mask, "Sedtype_do"] = "Vehicle"

_int3 = wv["intensity_do"].astype("string").str.strip().str.lower()
wv["intensity3_do"] = _int3.where(~_int3.isin(["moderate", "vigorous"]), "mvpa")
wv["intensity4_do"] = wv["intensity_do"]

print("\n=== CURRENT WavesReady output for non_codable rows ===")
nc_wv = wv[wv["activity_type"] == "non_codable"].head(10)
print(nc_wv[["activity_type", "posture_wbm", "domain_do", "posture_do",
             "intensity3_do", "intensity4_do", "Sedtype_do"]].to_string())

# ------------------------------------------------------------
# Simulate PROPOSED fix:
# Rule: when posture_wbm is NA or "not_coded",
# blank out posture_do, intensity3_do, intensity4_do, steps_do, Sedtype_do.
# domain_do is independent and is NOT affected.
# ------------------------------------------------------------
bad_posture_mask = posture_norm.eq("not_coded") | wv["posture_wbm"].isna()

wv["posture_do"]    = wv["posture_do"].where(~bad_posture_mask, pd.NA)
wv["intensity3_do"] = wv["intensity3_do"].where(~bad_posture_mask, pd.NA)
wv["intensity4_do"] = wv["intensity4_do"].where(~bad_posture_mask, pd.NA)
wv["Sedtype_do"]    = wv["Sedtype_do"].where(~bad_posture_mask, pd.NA)
# steps_do is only created during WavesReady export (not in the regular file)
if "steps_do" in wv.columns:
    wv["steps_do"] = wv["steps_do"].where(~bad_posture_mask, pd.NA)

print("\n=== PROPOSED FIX: non_codable rows (posture=not_coded) ===")
nc_fixed = wv[wv["activity_type"] == "non_codable"].head(10)
print(nc_fixed[["activity_type", "posture_wbm", "domain_do", "posture_do",
                "intensity3_do", "intensity4_do", "Sedtype_do"]].to_string())

print("\n=== PROPOSED FIX: mixed rows (valid activity + not_coded posture) ===")
mixed_fixed = wv[posture_norm.eq("not_coded") & activity_norm.ne("non_codable")].head(10)
print(mixed_fixed[["activity_type", "posture_wbm", "domain_do", "posture_do",
                   "intensity3_do", "intensity4_do", "Sedtype_do"]].to_string())

print(f"\nTotal rows affected by fix: {bad_posture_mask.sum()}")
print(f"  - non_codable activity + not_coded posture: {(activity_norm.eq('non_codable') & posture_norm.eq('not_coded')).sum()}")
print(f"  - valid activity + not_coded posture:       {(activity_norm.ne('non_codable') & posture_norm.eq('not_coded')).sum()}")
print(f"  - NA posture:                               {wv['posture_wbm'].isna().sum()}")

print("\nDone.")
