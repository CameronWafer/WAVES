import pandas as pd

am_ground = pd.read_csv(
    "C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv",
    low_memory=False,
)
am_gt = pd.read_csv("C:/Users/HELIOS-300/Desktop/Data/am_gt_3.csv", low_memory=False)

print("am_ground.head():")
print(am_ground.head(), "\n")
print("am_gt.head():")
print(am_gt.head(), "\n")

print("am_ground.columns:")
print(list(am_ground.columns), "\n")
print("am_gt.columns:")
print(list(am_gt.columns), "\n")

print("am_ground.dtypes:")
print(am_ground.dtypes, "\n")
print("am_gt.dtypes:")
print(am_gt.dtypes, "\n")

print("am_ground.shape:", am_ground.shape)
print("am_gt.shape:", am_gt.shape, "\n")

print("am_ground id unique sample (20):", am_ground["id"].dropna().unique()[:20])
print("am_ground do_session unique:", am_ground["do_session"].dropna().unique(), "\n")

print("am_gt id unique sample (20):", am_gt["id"].dropna().unique()[:20])
print("am_gt DO_session unique:", am_gt["DO_session"].dropna().unique(), "\n")

time_cols_ground = [c for c in am_ground.columns if "time" in c.lower()]
time_cols_gt = [c for c in am_gt.columns if "time" in c.lower()]
print("am_ground time-ish columns:", time_cols_ground)
print("am_gt time-ish columns:", time_cols_gt, "\n")

for col in time_cols_ground:
    print(f"am_ground[{col}] sample values:", am_ground[col].dropna().astype(str).head(5).tolist())
for col in time_cols_gt:
    print(f"am_gt[{col}] sample values:", am_gt[col].dropna().astype(str).head(5).tolist())
