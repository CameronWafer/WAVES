import pandas as pd
import numpy as np

df = pd.read_csv("c:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv")

# ── Apply Fix 1: sed_posture_map no longer maps not_coded -> active ──────────
df.loc[df["posture_wbm"] == "not_coded", "sed.posture_do"] = pd.NA

# ── Apply Fix 2: clear forward-filled intensity for not_coded / NA posture ───
bad_posture = (df["posture_wbm"] == "not_coded") | df["posture_wbm"].isna()
df.loc[bad_posture, "intensity_do"] = pd.NA

print("=== REGULAR FILE after fixes ===")
print(f"Total rows: {len(df)}")
print()

reg_cols = ["activity_type", "broad_domain", "broad.behavior_do",
            "posture_wbm", "posture_broad", "broad.posture_do",
            "sed.posture_do", "intensity_do"]

for col in reg_cols:
    n_blank = df[col].isna().sum()
    if n_blank > 0:
        print(f"  {col}: {n_blank} blank rows")

print()
print("  Breakdown of blank rows by column:")
blank_mask = df[reg_cols].isna().any(axis=1)
print(f"  Rows with ANY blank in key columns: {blank_mask.sum()}")
print()
print("  posture_wbm blank breakdown:")
print(df["posture_wbm"].value_counts(dropna=False).tail(3))

# ── Simulate WavesReady export with Fix 3 ────────────────────────────────────
wv = df.copy()
wv["site"] = "CP"
wv["pid"] = wv["id"]
wv["observation"] = wv["obs"].astype("string")

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
wv["steps_do"] = pd.NA

# Fix 3: blank posture-dependent columns when posture is not_coded or NA
_bad = posture_norm.eq("not_coded") | wv["posture_wbm"].isna()
for col in ["posture_do", "Sedtype_do", "intensity3_do", "intensity4_do", "steps_do"]:
    wv.loc[_bad, col] = pd.NA

print()
print("=== WAVESREADY FILE after fixes ===")
wr_cols = ["domain_do", "posture_do", "intensity3_do", "intensity4_do", "steps_do", "Sedtype_do"]

for col in wr_cols:
    n_blank = wv[col].isna().sum()
    if n_blank > 0:
        print(f"  {col}: {n_blank} blank rows")
    else:
        print(f"  {col}: 0 blank rows ✓")

print()
print("  Rows with ANY blank in WavesReady key columns:",
      wv[wr_cols].isna().any(axis=1).sum())
print()
print("  All blank rows belong to not_coded/NA posture?",
      wv[wr_cols].isna().any(axis=1).eq(_bad).all())
