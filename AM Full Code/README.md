# AM Data Cleaning Pipeline

**File:** `AM_restart1.ipynb`

This notebook processes raw AM (Actigraph Monitor) behavioral observation data into a second-by-second, fully encoded dataset ready for WAVES analysis. It rebuilds absolute timestamps from log anchors, expands event-level coding to per-second resolution, and encodes behavioral labels into standardized activity type, posture, domain, and intensity columns.

---

## Inputs

| File | Description |
|------|-------------|
| `am_behposture_onesheet.xlsx` | Raw AM behavioral and posture event data (single-sheet Excel, from BORIS or equivalent coding software) |
| `DO_LOG_final.csv` | Session log containing participant IDs, DO session labels, session dates, and start/stop times |

---

## Outputs

| File | Description |
|------|-------------|
| `Cameron_AM_Clean.csv` | Full cleaned dataset with all encoded behavioral and posture columns |
| `Cameron_AM_Clean_WavesReady.csv` | Codebook-style output formatted for WAVES database entry |

---

## Processing Steps

### 1. Data Import and Initial Filtering
- Loads behavioral data from Excel and the session log from CSV.
- Filters behavioral data to keep only `State start` rows.
- Converts any timedelta-formatted columns (e.g., `"0 days 00:00:00"`) back to plain `HH:MM:SS` strings.

### 2. Participant and Session-Specific Data Cleanups
Several targeted fixes are applied before general cleaning:

- **AM24 DO2 duplicates**: Two observation files exist for this session. The `M` version (`AM24DO2_M_FINAL_R`) is dropped entirely; the `R_FINAL_C` version is kept.
- **AM26 DO2 duplicates**: All rows from `AM26DO2_R_copyB_FINAL_C` are dropped.
- **AM11 DO1 negative times**: Rows with negative `Time_Relative_hms` in `AM11DO1_N_copyB_FINAL_C` are mostly dropped. The single exception is a `LES- screen based` row starting at hour 13, which is retained and reset to `00:00:00` as the session start.
- **Global negative time drop**: Any remaining rows with a negative `Time_Relative_hms` across all observations are dropped.

### 3. ID and DO Label Extraction
**From the log (`log_df`):**
- Extracts the 2-digit numeric participant ID from strings like `"AM02"`.
- Uses the `obs` column directly as the `do` label (e.g., `DO1`, `DO2`, `DO2_a`, `DO2_b`).
- Applies the **AM10 DO2 manual override**: forces `start_time = 11:20:00` and `stop_time = 13:21:00`.
- For participants 11 and 26, if their log still shows a plain `DO2` (rather than `DO2_a`/`DO2_b`), the two rows are split by chronological start time.

**From the behavioral data (`behav_am_df`):**
- Extracts participant ID from the `Observation` string (2 digits after `AM`).
- Extracts the DO session label using a regex pattern (`DO\d+(?:_[ab])?`).
- For participants 11 and 26, splits behavioral rows labeled `DO2` into `DO2_a` / `DO2_b` using an absolute timestamp cutoff specific to each participant.

Both dataframes also get a `do_base` column that strips trailing `_a`/`_b` suffixes, used as the join key for log matching.

### 4. Log Preparation
- Parses `start_date` from separate year/month/day columns.
- Constructs `start_dt` (datetime) and `stop_dt` from date and time fields.
- Handles overnight sessions (stop time the next day) by detecting when `stop_dt < start_dt` and adding one day.
- Computes `dur_log_s`: total session duration in seconds.

### 5. Observation-to-Log Mapping
Each behavioral observation file is matched to a corresponding log row:
- Groups observations and log rows by `(id_num, do_base)`.
- Computes the union coverage duration (in seconds) for each observation from its event start and end times.
- Matches observations to log rows by minimizing the absolute difference between behavioral coverage duration and log session duration. Uses exact permutation search for small groups (≤ 8) and a greedy algorithm for larger ones.
- Observations with no matching log group are flagged as `missing_log_group`.
- Observations in excess of available log rows are flagged as `unmapped_extra_observation`.

### 6. Absolute Time Reconstruction
- Merges the log mapping (start datetime, stop datetime, duration, map status) onto the behavioral rows.
- Normalizes each observation's relative seconds so the first event is at second 0 (`rel_norm_s`).
- Reconstructs absolute datetime columns (`Date_Time_Absolute_dmy_hmsf`, `Time_Absolute_hms`) by adding `rel_norm_s` to the matched log `start_dt`.
- Drops rows that cannot be anchored (no mapped log entry).

### 7. Per-Second Grid Construction
For each observation, a second-by-second grid is built from the reconstructed absolute start to end:
- At each second on the grid, the latest-starting behavioral event that is still active at that second is selected (via binary search).
- Two separate streams are tracked simultaneously:
  - **Activity stream** (`_behavior_activity_raw`): domain-level behaviors (work, household, leisure, travel, etc.)
  - **Posture stream** (`_behavior_posture_raw`): physical position/movement behaviors (sit, stand, walk, etc.)
- Posture-side modifiers (`Modifier_1` through `Modifier_4`) are also captured from posture stream rows.

The result is a `sec_by_sec` dataframe with one row per second per observation (360,857 rows, 56 unique observations).

### 8. Column Cleanup and Renaming
Raw time columns are dropped and consolidated:
- `date_time_abs` → `date_time`
- `time_abs_hms` → `time`
- `time_rel` → `rel_time`
- `Duration_sf` → `duration`

A `do_session` column is assigned from the mapped log label (preserving `DO2_a`/`DO2_b` distinctions). A `time_relative_new` column is computed as the elapsed seconds from each observation's first timestamp.

### 9. Forward-Fill of Behaviors and Modifiers
Within each observation, `Behavior`, `Modifier_1`, `Modifier_2`, `Modifier_3`, and `Modifier_4` are forward-filled. This carries the last-seen value forward until a new event starts, representing the assumption that a behavior continues until a new one is coded.

### 10. Behavior Splitting into Activity and Posture
Two new columns are derived from the separate behavior streams:
- `Activity_Type`: mapped from `_behavior_activity_raw` using the activity lookup table.
- `Posture`: mapped from `_behavior_posture_raw` using the posture lookup table.

Both are then forward-filled within each observation.

**Activity map** (raw label → code):

| Raw Behavior | `Activity_Type` |
|---|---|
| `SL- sleep` | `sleep` |
| `WRK- general` | `work_general` |
| `WRK- screen based` | `work_screen` |
| `LES- socializing...` | `les_social` |
| `LES- screen based...` | `les_screen` |
| `EX- participating in sport...` | `ex_sport` |
| `TRAV- biking` | `trav_bike` |
| `TRAV- walking` | `trav_walk` |
| `HA- housework` | `ha_housework` |
| `OTHER- non codable` | `non_codable` |
| *(and more)* | |

**Posture map** (raw label → code):

| Raw Behavior | `Posture` (→ `posture_wbm`) |
|---|---|
| `SB-sitting` | `sitting` |
| `SB- lying` | `lying` |
| `LA- stand` | `stand` |
| `LA- stand and move` | `stand_move` |
| `WA- walk` | `walk` |
| `WA- ascend stairs` | `ascend` |
| `WA- descend stairs` | `descend` |
| `WA- running` | `running` |
| `SP- bike` | `biking` |
| `SP- muscle strengthening` | `muscle_strength` |
| *(and more)* | |

### 11. Activity Refinements
- **EX sport subtype**: When `Activity_Type == ex_sport` and `Modifier_2` contains a sport name, the code is refined to `EX-{sport}` (e.g., `EX-hiking`, `EX-basketball`).
- **Work subtype**: When `Activity_Type` is `work_general` or `work_screen`, `Modifier_4` is parsed and stored in a separate `work_type` column (e.g., `work_education`, `work_construction`).

### 12. Comment Carry-Forward
`Comment` is forward-filled only within stable activity/posture segments. When a new `Activity_Type` or `Posture` value is detected within an observation, the comment is reset rather than carried across the boundary.

### 13. Domain and Posture Encoding
**Activity_Type → domain columns:**

| Column | Example | Description |
|--------|---------|-------------|
| `broad_domain` | `work_education`, `leisure`, `Trav_car` | Mid-level domain grouping |
| `waves_domain` | `occupation`, `leisure_inactive`, `travel_inactive` | Top-level WAVES domain |

**posture_wbm → posture columns:**

| Column | Example | Description |
|--------|---------|-------------|
| `posture_broad` | `sedentary`, `stand_move`, `walk` | Broad posture category |
| `posture_waves` | `sedentary`, `stationary`, `walking` | WAVES posture label |

**posture_wbm + Activity_Type → `sed.posture_do`:**
- `sedentary`: sitting (non-vehicle), lying
- `sed_drive`: sitting while driving or as a passenger
- `active`: all other postures

### 14. Session-Specific Post-Processing
- **AM02 DO2_b**: All rows for this split session are dropped. `DO2_a` is relabeled to `DO2`.
- **AM10 DO2 cutoff**: All rows with `rel_time > 02:01:00` for AM10 DO2 are dropped, enforcing the manually specified session end.

### 15. Intensity Derivation
`intensity_do` is derived in this priority order:
1. `Modifier_3` text: `"vigorous"` → `vigorous`; `"moderate"` → `moderate`
2. Posture override rules (always applied for sedentary/light postures):
   - `sitting`, `lying`, `kneel_squat` → `sedentary`
   - `stand`, `stretch` → `light`
3. Posture fill rules for remaining missing values:
   - `stand_move` → `light`
   - `walk`, `walk_load`, `ascend`, `descend` → `moderate`
   - `running`, `biking`, `sport_move`, `muscle_strength` → `vigorous`

### 16. Final Column Schema
The final `waves_df_clean` dataframe contains:

`id`, `obs`, `date`, `date_time`, `rel_time`, `activity_type`, `broad_domain`, `broad.behavior_do`, `posture_wbm`, `posture_broad`, `broad.posture_do`, `sed.posture_do`, `intensity_do`

### 17. Export

**`Cameron_AM_Clean.csv`** — Full output:
All columns listed above. Note: `work_type` is computed internally but dropped before export.

**`Cameron_AM_Clean_WavesReady.csv`** — Codebook-style output:
`site`, `pid`, `observation`, `date_time`, `date`, `time`, `domain_do`, `posture_do`, `intensity3_do`, `intensity4_do`, `steps_do`, `Sedtype_do`

Additional derived columns in the WavesReady export:
- `domain_do`: collapses `broad_domain` into 5 top-level groups (`leisure`, `household`, `transportation`, `occupation`, `other`)
- `posture_do`: maps `broad.posture_do` to codebook values (`sedentary`, `mixed_movement`, `walking`, `running`, `biking`)
- `Sedtype_do`: classifies each second as `non_sedentary`, `sit_lie`, `Lying`, or `Vehicle`
- `intensity3_do`: combines `moderate` and `vigorous` into `mvpa`
- `intensity4_do`: 1-to-1 copy of `intensity_do` (4 levels: sedentary, light, moderate, vigorous)
- `steps_do`: set to `"NA"` for all AM rows (no step data in this pipeline)

---

## Key Design Decisions

- **Absolute time reconstruction from log**: Rather than trusting the raw relative time strings in the behavioral file, the pipeline re-derives absolute time by anchoring to the log's validated start datetime. This corrects for clock drift or offset issues in the original coding files.
- **Duration-based observation-to-log matching**: Observations are matched to log rows by comparing behavioral coverage duration to log session duration, not by name. This is robust to naming inconsistencies and handles split sessions (copyA/copyB) correctly.
- **Dual-track per-second expansion**: Activity and posture behaviors are tracked in separate streams throughout the expansion. This prevents a posture event from overwriting an activity event at the same second, preserving both in the output.
- **No step data**: The AM pipeline does not include accelerometer step counts. The `steps_do` column in the WavesReady export is always `"NA"`.
