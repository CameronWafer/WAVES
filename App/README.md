# WAVES Processor

Windows desktop application that runs the ActiNet, Accelerometer, and Step Count accelerometer processing pipelines without requiring the user to install Python, Conda, Java, or any developer tools.

Current version: **1.4**

> Shortcut on dev machine: `"D:\WAVES Processor\WAVES Processor.exe"`

---

## For users

1. Install using `WAVES_Processor_Setup_v1.4.exe`.
2. Open **WAVES Processor** from the desktop shortcut.
3. Select the folder containing your accelerometer files (`.gt3x`, `.bin`, or `.cwa`).
4. Select or confirm the output folder.
5. Check **ActiNet**, **Accelerometer**, **Step Count (SSL)**, **Step Count (RF)**, or any combination.
6. Click **Run Selected Processes**.
7. Wait. Open the output folder when done.

Outputs go to `Documents\WAVES Outputs\<timestamp>\` by default.
Logs go to `%LOCALAPPDATA%\WAVES Processor\logs\`.

**Supported file types by pipeline:**

| Pipeline | .gt3x | .bin | .cwa |
|---|---|---|---|
| ActiNet | Yes | Yes | No |
| Accelerometer | Yes | Yes | No |
| Step Count | Yes | Yes | Yes |

---

## For developers (build machine setup)

### Requirements

- Windows 10 or 11
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Miniforge](https://github.com/conda-forge/miniforge) installed
- Internet access
- [Inno Setup 6](https://jrsoftware.org/isinfo.php) installed

Install developer tools once:

```cmd
conda install -n base conda-pack
pip install pyinstaller pytest
```

---

## Build steps (run in order)

All scripts are in `build_scripts\`. Run from **Anaconda Prompt** unless noted.

### Step 1 — Build environments

```cmd
build_scripts\1_build_envs.bat
```

Creates the `WAVES_actinet`, `WAVES_accelerometer`, and `WAVES_stepcount` Conda environments.

### Step 2 — Smoke-test environments

```cmd
build_scripts\2_test_envs.bat
```

Checks that all three environments respond correctly.

**Also test on a real file before packing:**

```cmd
conda activate WAVES_actinet
python -m actinet.actinet C:\path\to\test.gt3x -o C:\path\to\test_outputs\actinet -p

conda activate WAVES_accelerometer
python -m accelerometer.accProcess C:\path\to\test.gt3x -o C:\path\to\test_outputs\accelerometer

conda activate WAVES_stepcount
python -m stepcount.stepcount C:\path\to\test.gt3x -o C:\path\to\test_outputs\stepcount
```

Confirm output files appear. **Do not proceed to Step 3 until all three pass.**

### Step 3 — Pack environments

```cmd
build_scripts\3_pack_envs.bat
```

Packs and unpacks all three environments into `release_build\WAVES Processor\envs\`.

**Critical test after this step** — run the unpacked `python.exe` files directly without any conda activation:

```cmd
"release_build\WAVES Processor\envs\WAVES_actinet\python.exe" -m actinet.actinet --help
"release_build\WAVES Processor\envs\WAVES_accelerometer\python.exe" -m accelerometer.accProcess --help
"release_build\WAVES Processor\envs\WAVES_stepcount\python.exe" -m stepcount.stepcount --help
```

All three must print help output. If any fail here, the app will fail on the user's machine.

> **Note:** The app invokes `python.exe -m <module>` rather than the `.exe` launcher scripts
> (`actinet.exe`, `accProcess.exe`). This bypasses the conda-unpack path-fixup problem —
> `python.exe` is a native binary with no hardcoded install paths. See `HANDOFF.md` for history.

### Step 4 — Build the app

```cmd
build_scripts\4_build_app.bat
```

Runs PyInstaller, then copies `config.json`, `resources\`, `envs\`, and `logs\` into `dist\WAVES Processor\`.

Test the built app directly:

```cmd
"dist\WAVES Processor\WAVES Processor.exe"
```

### Step 5 — Build the installer

```cmd
build_scripts\5_build_installer.bat
```

Compiles `installer\waves_processor.iss` and creates `installer\Output\WAVES_Processor_Setup_v1.4.exe`.

---

## Running tests

```cmd
python -m pytest tests/ -v
```

Tests cover validation logic and command construction (no external processes executed).

> See `tests/test_command_building.py` for a note on current test coverage.

---

## Project structure

```
App/
├── config.json                  App configuration — versions, paths, flags
├── build_envs/
│   ├── actinet_environment.yml
│   ├── accelerometer_environment.yml
│   └── stepcount_environment.yml
├── src/
│   ├── waves_gui.py             Entry point, tkinter GUI
│   ├── waves_runner.py          Subprocess execution logic
│   ├── waves_config.py          Path and config resolution
│   ├── waves_logging.py         Structured run logger
│   └── waves_validation.py      Pre-run input validation
├── build_scripts/
│   ├── 1_build_envs.bat
│   ├── 2_test_envs.bat
│   ├── 3_pack_envs.bat
│   ├── 4_build_app.bat
│   └── 5_build_installer.bat
├── installer/
│   └── waves_processor.iss      Inno Setup script
├── tests/
│   ├── test_validation.py
│   └── test_command_building.py
├── resources/                   Icons and optional docs
├── logs/                        Runtime logs (gitignored)
└── release_build/               Packed environments (gitignored)
```

**Not tracked by git (too large):**
- `envs/` — packed Conda environments (1–3 GB each)
- `release_build/` — intermediate build output
- `dist/` — PyInstaller output
- `build/` — PyInstaller temp files

---

## Updating package versions

1. Edit the relevant file in `build_envs\` (`actinet_environment.yml`, `accelerometer_environment.yml`, or `stepcount_environment.yml`).
2. Edit `config.json` to match the new version strings.
3. Bump `app_version` in `config.json`.
4. Delete the old Conda environment and rebuild from Step 1.
5. Re-test, re-pack, re-build, re-install.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `python.exe` not found at startup | Environments not packed into `envs\` | Run Steps 3–4 |
| `java.exe` not found error in log | JAVA_HOME not set or JRE missing from packed env | Check `envs\WAVES_actinet\Library\jre\` exists |
| App works in dev, fails installed | Environment not properly packed | Run Steps 3–4 again; test critical step after Step 3 |
| `accProcess` takes wrong arguments | CLI changed in the pinned version | Run `python -m accelerometer.accProcess --help` and compare to `config.json` |
| Step Count not appearing in GUI | `stepcount` section missing or `enabled: false` in `config.json` | Check `config.json` |
| Antivirus flags the exe | PyInstaller false positive | Expected — inform IT or consider code signing |

---

## Versions

| App version | actinet | accelerometer | stepcount |
|---|---|---|---|
| 1.0.0 | 0.7.0 | 7.2.3 | — |
| 1.2 | 0.7.0 | 7.2.3 | — |
| 1.4 | 0.7.0 | 7.2.3 | 3.18.2 |
