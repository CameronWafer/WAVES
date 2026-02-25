# AM Data Accuracy Report

This report lists data issues that block 100% accurate merging. I took Saturday 2/21/2026 to try and keep the same logic from ACT24, making changes where needed to adapt correctly. It still kept producing errors, even with fixing the mapping (mapping is good now, it had spelling mismatches).

What is in this report are things that keep causing problems. I am not sure how am_gt_3 was created so seemlessly without making major assumptions. Who made am_gt_3


## Quick totals

- Total behavior observations checked: 61

- Total log session rows: 57

- Session groups flagged for manual decision: 11


## 1) Missing in log file

- We are missing `AM10 DO1` in `log_df`.


## 2) AM15 DO2 detailed example

- Behavior has 4 observations.

- Log has 2 rows.

- The 4 behavior observations are:

  - `AM15DO2_M_copyA_FINAL_C`: observed coverage duration 01:35:03 (5703.1s); negative relative time offset 00:00:00 (0.0s)

  - `AM15DO2_M_copyB_FINAL_C`: observed coverage duration 01:57:23 (7042.6s); negative relative time offset 01:35:51 (5751.1s)

  - `AM15DO2_N_copyA_FINAL_R`: observed coverage duration 01:35:04 (5703.6s); negative relative time offset 00:00:00 (0.0s)

  - `AM15DO2_N_copyB_FINAL`: observed coverage duration 02:02:01 (7321.1s); negative relative time offset 01:36:51 (5811.1s)

- Log rows for AM15 DO2 are:

  - `DO2_a`: log duration 01:35:02 (5702.0s) (start 2017-10-13 15:51:58, stop 2017-10-13 17:27:00)

  - `DO2_b`: log duration 00:21:30 (1290.0s) (start 2017-10-13 17:28:00, stop 2017-10-13 17:49:30)


## 3) Observations with negative relative time values

- Count: 10 observations.

- `AM02DO2_J_copyB_FINAL_R` (AM02 DO2): 4 rows with negative relative time; largest negative offset 00:52:16 (3136.0s).

- `AM08DO1_J_copyB_FINAL_R` (AM08 DO1): 2 rows with negative relative time; largest negative offset 01:16:49 (4609.4s).

- `AM09DO1_N_FINAL_R` (AM09 DO1): 2 rows with negative relative time; largest negative offset 00:06:30 (390.4s).

- `AM11DO1_N_copyB_FINAL_C` (AM11 DO1): 6 rows with negative relative time; largest negative offset 00:51:39 (3099.0s).

- `AM11DO2_R_copyB_FINAL_C` (AM11 DO2): 3 rows with negative relative time; largest negative offset 01:51:53 (6712.8s).

- `AM12DO2_J_copyA_FINAL_C` (AM12 DO2): 2 rows with negative relative time; largest negative offset 00:00:10 (10.1s).

- `AM12DO2_J_copyB_FINAL_C` (AM12 DO2): 4 rows with negative relative time; largest negative offset 01:15:58 (4558.3s).

- `AM15DO2_M_copyB_FINAL_C` (AM15 DO2): 2 rows with negative relative time; largest negative offset 01:35:51 (5751.1s).

- `AM15DO2_N_copyB_FINAL` (AM15 DO2): 71 rows with negative relative time; largest negative offset 01:36:51 (5811.1s).

- `AM26DO2_R_copyB_FINAL_C` (AM26 DO2): 144 rows with negative relative time; largest negative offset 01:44:13 (6253.1s).


## 4) Groups needing manual decision (non-deterministic)

- These groups cannot be resolved 100% accurately without additional rules or source corrections.

- `AM02 DO2`: duration mismatch: behavior 03:00:39 (10839.2s) vs log 02:08:21 (7701.0s); negative relative time exists (sum offset 00:52:16 (3136.0s)).

- `AM08 DO1`: duration mismatch: behavior 03:17:05 (11825.5s) vs log 02:00:14 (7214.0s); negative relative time exists (sum offset 01:16:49 (4609.4s)).

- `AM09 DO1`: duration mismatch: behavior 01:49:15 (6555.2s) vs log 01:42:44 (6164.0s); negative relative time exists (sum offset 00:06:30 (390.4s)).

- `AM10 DO1`: missing in log; more behavior observations than log rows.

- `AM10 DO2`: duration mismatch: behavior 02:02:29 (7349.4s) vs log 01:18:10 (4690.0s).

- `AM11 DO1`: duration mismatch: behavior 03:56:23 (14182.7s) vs log 01:55:32 (6932.0s); negative relative time exists (sum offset 00:51:39 (3099.0s)).

- `AM11 DO2`: multiple log rows are unsuffixed (no _a/_b); duration mismatch: behavior 03:53:56 (14035.6s) vs log 02:02:02 (7322.0s); negative relative time exists (sum offset 01:51:53 (6712.8s)).

- `AM12 DO2`: duration mismatch: behavior 03:18:37 (11916.8s) vs log 02:02:36 (7356.0s); negative relative time exists (sum offset 01:16:08 (4568.4s)).

- `AM15 DO2`: more behavior observations than log rows; duration mismatch: behavior 07:09:30 (25770.4s) vs log 01:56:32 (6992.0s); negative relative time exists (sum offset 03:12:42 (11562.2s)).

- `AM24 DO2`: more behavior observations than log rows; duration mismatch: behavior 04:00:03 (14403.0s) vs log 02:00:00 (7200.0s).

- `AM26 DO2`: multiple log rows are unsuffixed (no _a/_b); duration mismatch: behavior 03:44:23 (13463.5s) vs log 01:56:09 (6969.0s); negative relative time exists (sum offset 01:44:13 (6253.1s)).


## 5) What this means

- Any pipeline step that auto-combines these flagged groups will require assumptions.

- To stay 100% accurate, each flagged group should be resolved manually (or with new source data).