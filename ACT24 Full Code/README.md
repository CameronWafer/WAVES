# ACT24 Data Cleaning Pipeline

**File:** `dataCleanOneChunk_ACT.ipynb`

This notebook processes raw ACT24 behavioral observation data into a second-by-second, fully encoded dataset ready for WAVES analysis. It takes event-level behavioral coding files and expands them into a per-second time series with standardized activity type, posture, domain, intensity, and step count columns.

---

## Inputs

| File | Description |
|------|-------------|
| `ACT24_behposture_event(in).csv` | Raw ACT24 behavioral and posture event data (from BORIS or equivalent coding software) |
| `do_log_final_behavior(in).csv` | Session log containing participant IDs, observation numbers, session dates, and start times |
| `seconds_ground_truth_20250410.csv` | Ground-truth step count data at per-second resolution |

---

## Outputs

| File | Description |
|------|-------------|
| `Cameron_ACT24_Clean_NoDrop.csv` | Full cleaned dataset with all encoded behavioral and posture columns |
| `Cameron_ACT24_Clean_WavesReady_NoDrop.csv` | Codebook-style output formatted for WAVES database entry |

---

## Processing Steps

### 1. Data Import and Initial Filtering
- Loads the behavioral event CSV and the session log CSV.
- Excludes participant ID 135 from all processing.
- Filters the behavioral data to keep only `State start` rows (drops `State stop` and other event types).

### 2. Log Cleaning and ID Standardization
- Parses the log's separate year/month/day columns into a single date field.
- Standardizes the `start_time` column to 24-hour `HH:MM:SS` format, handling both `H:MM AM/PM` and `H:MM:SS AM/PM` formats.
- Extracts numeric participant `id` and observation number `do` from the `Observation` string (format: `ID_###_##_X`).

### 3. Session Start Time Merge
- Merges the log's `start_time` into the behavioral dataframe using `id` and `do` as join keys.
- Computes an absolute event start time (`start_time_new`) by adding each event's relative offset (`Time_Relative_hmsf`) to the session start time from the log.

### 4. Behavior Track Classification
Each behavior code is classified into one of two independent tracks based on its prefix:

**Activity track** (`activity` — domain-level behaviors):
- Prefixes: `sl-`, `pc-`, `ha-`, `ca-`, `wrk-`, `edu-`, `org-`, `pur-`, `eat-`, `les-`, `ex-`, `trav-`, `other-`

**Posture track** (`posture` — physical position/movement):
- Prefixes: `sb-`, `la-`, `wa-`, `sp-`

Rows that don't match either track (e.g., `start behavior`, `start posture`, `private/not coded`) are classified as `other` and handled separately.

### 5. Per-Second Expansion
Each track is independently expanded from event-level to per-second resolution:
- Converts each event's relative time to seconds.
- Uses event durations (`Duration_sf`) to determine the end time of each event.
- Builds a per-second grid from session start to end.
- At each second, the latest-starting active event is selected (forward-fill by last event).

### 6. Activity and Posture Track Merge
- Performs a full outer merge of the activity-expanded and posture-expanded dataframes on `(Observation, second)`.
- Regenerates `Time_Relative_hms_new` from the integer second offset to avoid forward-fill duplication.
- Combines `Modifier_2` (intensity): posture track takes priority, with the activity track as fallback.
- Carries forward `id`, `do`, and `start_time_new` metadata within each observation.

### 7. Activity Encoding
Raw behavior labels are mapped to three hierarchical encoded columns:

| Column | Example | Description |
|--------|---------|-------------|
| `activity_type` | `work_general`, `trav_walk`, `ex_sport` | Standardized activity code |
| `broad_domain` | `work_education`, `leisure`, `active_transportation` | Mid-level domain grouping |
| `waves_domain` | `occupation`, `leisure_inactive`, `active_time` | Top-level WAVES domain |

Special cases:
- **EX sport rows**: the specific sport subtype from `Modifier_1` is appended to form codes like `EX-hiking`.
- **Work rows**: the industry sector from `Modifier_3` is stored in a separate `work_type` column (e.g., `work_education_and_health_services`).

### 8. Posture Encoding
Raw posture behavior labels are mapped to three encoded columns:

| Column | Example | Description |
|--------|---------|-------------|
| `posture_wbm` | `sitting`, `stand`, `walk`, `running` | Specific posture/movement code |
| `posture_broad` | `sedentary`, `stand_move`, `walk` | Broad posture category |
| `posture_waves` | `sedentary`, `stationary`, `walking` | WAVES posture label |

Additionally:
- `waves_sedentary`: classifies each second as `sedentary`, `active`, or `sed_drive` (sitting while driving/as a passenger).

### 9. Intensity Encoding
`intensity` is derived in priority order:
1. Posture type (sitting/lying/kneeling → `sedentary`; standing/stretching → `light`)
2. `Modifier_2` text (vigorous, moderate, light, sedentary)
3. Posture-based fallback rules after stabilization (walk → `moderate`; running/biking/sport → `vigorous`)

`waves_intensity` collapses moderate and vigorous into `mvpa`.

### 10. Stabilization
After initial encoding, forward-fill and backward-fill are applied to `Activity_Type` and `posture_wbm` within each observation to eliminate any remaining gaps. All derived columns (`broad_domain`, `waves_domain`, `posture_broad`, `posture_waves`, `waves_sedentary`, `intensity`, `waves_intensity`) are then recomputed from the stabilized base values.

### 11. Steps Integration
- Loads a ground-truth step count CSV and standardizes `rel_time` formatting to `HH:MM:SS`.
- Merges step data onto the cleaned dataset using `(id, obs, rel_time)` as the join key.
- Missing step values are written as literal `"NA"` in the output files.

### 12. Column Renaming
Final columns are renamed to match the WAVES codebook schema:

| Original | Renamed |
|----------|---------|
| `waves_domain` | `broad.behavior_do` |
| `posture_waves` | `broad.posture_do` |
| `waves_sedentary` | `sed.posture_do` |
| `intensity` | `intensity_do` |

### 13. Export

**`Cameron_ACT24_Clean_NoDrop.csv`** — Full output (490,080 rows):
`id`, `obs`, `date`, `date_time`, `rel_time`, `activity_type`, `broad_domain`, `broad.behavior_do`, `posture_wbm`, `posture_broad`, `broad.posture_do`, `sed.posture_do`, `intensity_do`, `Quality`, `Step`

**`Cameron_ACT24_Clean_WavesReady_NoDrop.csv`** — Codebook-style output:
`site`, `pid`, `observation`, `date_time`, `date`, `time`, `domain_do`, `posture_do`, `intensity3_do`, `intensity4_do`, `steps_do`, `Sedtype_do`, `Quality`

Additional derived columns in the WavesReady export:
- `domain_do`: collapses `broad_domain` into 5 top-level groups (`leisure`, `household`, `transportation`, `occupation`, `other`)
- `posture_do`: maps `broad.posture_do` to codebook values (`sedentary`, `mixed_movement`, `walking`, `running`, `biking`)
- `Sedtype_do`: classifies each second as `non_sedentary`, `sit_lie`, `Lying`, or `Vehicle`
- `intensity3_do`: combines `moderate` and `vigorous` into `mvpa`; overridden to `non_codable` when `Quality == "Non-codeable"`
- `intensity4_do`: 1-to-1 copy of `intensity_do` (4 levels: sedentary, light, moderate, vigorous); overridden to `non_codable` when `Quality == "Non-codeable"`

---

## Post-Processing Pipeline

**File:** `maybe_fix_act24.ipynb`

This notebook applies additional fixes and data enrichment on top of `Cameron_ACT24_Clean_NoDrop.csv`. It is run as a separate step after the main pipeline and produces `act24_testing.csv` and `summary_act24_testing.csv`.

### Inputs

| File | Description |
|------|-------------|
| `Cameron_ACT24_Clean_NoDrop.csv` | Output of the main pipeline |
| `C:\Users\HELIOS-300\Desktop\Data\activPal ACT24\ACT24_###-*.csv` | Per-second activPal sensor files, one per participant (semicolon-delimited, `sep=;` header line) |

### Steps

1. **Remap `activity_type` to full labels** — Converts coded values (e.g. `work_general`) to full labels (e.g. `WRK- General`) using a backwards mapping table. Overwrites the `activity_type` column in place.

2. **Remap `broad_domain`** — Derives `broad_domain` from the new full-label `activity_type` using domain classification lists (household / occupation / leisure / transportation / other / non_codable). Case-insensitive matching; any unmatched labels are flagged as `"unmapped"`.

3. **Enforce non_codable intensity** — Where `activity_type == "OTHER- Non-Codable"`, sets `intensity_do = "non_codable"`.

4. **Merge activPal sensor data** — Loads all `ACT24_###` files from the activPal folder, extracts participant ID from the filename prefix, and left-merges onto the base file on `id` + `date_time`. Brings in all activPal columns: `StepCount`, `Activity Score (MET.s)`, `Sedentary Time (s)`, `Upright Time (s)`, `Stepping Time (s)`, `Cycling Time (s)`, `Primary Lying Time (s)`, `Secondary Lying Time (s)`, `Nonwear Time (s)`, `Seated Transport Time (s)`, `Data Errors (s)`, `Sedentary to Upright Movements`, `Upright to Sedentary Movements`, `Sum(abs(dChannel1/2/3))`.

5. **Nullify activPal columns for non_codable rows** — For rows where `activity_type == "OTHER- Non-Codable"`, all activPal columns are set to NaN.

### Outputs

| File | Description |
|------|-------------|
| `act24_testing.csv` | Base file with remapped labels and activPal columns merged in |
| `summary_act24_testing.csv` | 46-row summary (one per `id`+`obs`) with `gt_total_steps`, `gt_sedentary_s`, `ap_total_steps`, `ap_sedentary_s` |

---

## Key Design Decisions

- **Dual-track expansion**: Activity and posture behaviors are expanded independently before merging. This preserves simultaneous events at the same timestamp rather than overwriting one with the other.
- **Duration-based end time**: The posture track uses `Duration_sf` to determine how long each event lasts, rather than assuming it ends when the next event starts.
- **Stabilization pass**: A final ffill+bfill pass on both tracks ensures no gaps remain after the merge, without allowing values to bleed across observation boundaries.
- **Non-codable rows are retained** (not dropped) in these output files, marked with `activity_type = non_codable`.
