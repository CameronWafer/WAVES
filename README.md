# WAVES

This repo contains the data processing pipelines I built for the WAVES study. The main work here is a set of Jupyter notebooks that take raw behavioral observation data and transform it into second-by-second, WAVES-codebook-ready datasets for two cohorts: **AM** and **ACT24**.

I also built a Windows desktop app (WAVES Processor) that makes it easy for non-technical team members to run accelerometer processing tools without needing to install Python or Conda. The source code lives in `App/`. Build artifacts are excluded from git due to size. See `App/README.md` for build and usage instructions.

---

## Author

**Cameron Hafer**
cameroonhafer@gmail.com

---

## Repository Structure

```
WAVES/
├── AM Full Code/               AM behavioral observation pipeline
├── ACT24 Full Code/            ACT24 behavioral observation pipeline
├── CompareFiles/               QC and comparison scripts (see below)
├── ActivPal Full Code/         ActivPal sensor exploration
├── Steps Code/                 Step count exploration
├── Steps Data WAVES/steps/     Raw step event log Excel files (~40 files)
├── App/                        WAVES Processor desktop app (source only; build artifacts excluded)
└── README.md
```

---

## Input Data

The raw input files are **not included in this repository**. They were provided by Sarah Keadle and the WAVES Research Group. File paths in the notebooks are hardcoded to my local machine and will need to be updated if you're running this somewhere else.

---

## AM Pipeline

**Location:** `AM Full Code/`

This pipeline takes the raw AM (Actigraph Monitor) behavioral observation data and produces a clean, second-by-second dataset ready for WAVES analysis.

### File Guide

I'll be honest, the file names in here aren't intuitive at all. Here's what everything actually is:

| File | What it actually is |
|------|---------------------|
| `AM_restart1.ipynb` | **Main pipeline notebook**, run this first |
| `maybe_fix_am.ipynb` | **Post-processing notebook**, activPal sensor merge; run after main pipeline |
| `Cameron_AM_Clean.csv` | **Primary output** of `AM_restart1.ipynb`, full cleaned dataset |
| `Cameron_AM_Clean_WavesReady.csv` | **Codebook output** of `AM_restart1.ipynb`, formatted for WAVES database entry |
| `am_testing.csv` | Output of `maybe_fix_am.ipynb`, base data with activPal columns merged in |
| `summary_am_testing.csv` | Output of `maybe_fix_am.ipynb`, per-session summary comparing ground-truth vs activPal |
| `README.md` | Full step-by-step documentation of the pipeline |

### Run

1. Open and run all cells in **`AM_restart1.ipynb`** → produces `Cameron_AM_Clean.csv` and `Cameron_AM_Clean_WavesReady.csv`
2. Optionally, run **`maybe_fix_am.ipynb`** for the activPal sensor merge (requires local activPal CSV files)

### Inputs

| File | Description |
|------|-------------|
| `am_behposture_onesheet.xlsx` | Raw behavioral and posture event data from BORIS |
| `DO_LOG_final.csv` | Session log with participant IDs, session labels, dates, and start/stop times |

### Outputs

| File | Description |
|------|-------------|
| `Cameron_AM_Clean.csv` | Full cleaned dataset with all encoded columns |
| `Cameron_AM_Clean_WavesReady.csv` | Codebook-formatted output for WAVES database entry |

---

## ACT24 Pipeline

**Location:** `ACT24 Full Code/`

This pipeline takes the raw ACT24 behavioral observation data and produces a second-by-second dataset with activity type, posture, domain, intensity, and ground-truth step count columns.

### File Guide

Same story here, the names aren't obvious. Here's what everything actually is:

| File | What it actually is |
|------|---------------------|
| `dataCleanOneChunk_ACT.ipynb` | **Main pipeline notebook**, run this first |
| `maybe_fix_act24.ipynb` | **Post-processing notebook**, activPal merge, label remapping, non-codable handling; run after main pipeline |
| `checking_act24.ipynb` | Scratch validation notebook, not part of the main pipeline |
| `Cameron_ACT24_Clean_NoDrop.csv` | **Primary output** of `dataCleanOneChunk_ACT.ipynb`, full cleaned dataset |
| `Cameron_ACT24_Clean_WavesReady_NoDrop.csv` | **Codebook output** of `dataCleanOneChunk_ACT.ipynb`, formatted for WAVES database entry |
| `act24_testing.csv` | Output of `maybe_fix_act24.ipynb`, base data with activPal columns and remapped labels |
| `summary_act24_testing.csv` | Output of `maybe_fix_act24.ipynb`, per-session summary comparing ground-truth vs activPal |
| `README.md` | Full step-by-step documentation of the pipeline |

> **"NoDrop" in filenames** means non-codable rows are kept in the output (marked with `activity_type = non_codable`) rather than being dropped. This is the correct behavior for the current pipeline.

### Run

1. Open and run all cells in **`dataCleanOneChunk_ACT.ipynb`** → produces `Cameron_ACT24_Clean_NoDrop.csv` and `Cameron_ACT24_Clean_WavesReady_NoDrop.csv`
2. Optionally, run **`maybe_fix_act24.ipynb`** for the activPal sensor merge and label remapping

### Inputs

| File | Description |
|------|-------------|
| `ACT24_behposture_event(in).csv` | Raw behavioral and posture event data from BORIS |
| `do_log_final_behavior(in).csv` | Session log with participant IDs, observation numbers, session dates, and start times |
| `seconds_ground_truth_20250410.csv` | Ground-truth step count data at per-second resolution |

### Outputs

| File | Description |
|------|-------------|
| `Cameron_ACT24_Clean_NoDrop.csv` | Full cleaned dataset (490,080 rows) with all encoded columns |
| `Cameron_ACT24_Clean_WavesReady_NoDrop.csv` | Codebook-formatted output for WAVES database entry |

---

## Other Folders

These folders are **not needed to run the main pipelines**. They're mostly QC scripts and exploratory work I did along the way.

| Folder | Contents |
|--------|----------|
| `CompareFiles/` | QC scripts and comparison notebooks I used to validate pipeline outputs. Includes `AM_INTEGRITY_REPORT.md` documenting known data quality issues in the AM cohort. |
| `Steps Data WAVES/steps/` | Raw step event log Excel files for ~40 participants, used by `Steps Code/stepsExplore.ipynb`. |
| `Steps Code/` | Early step count exploration notebook. |
| `ActivPal Full Code/` | ActivPal sensor exploration for the PALS cycling study, separate from the main AM/ACT24 pipelines. |