# shortcut
# "D:\WAVES Processor\WAVES Processor.exe"

# WAVES Processor

Windows desktop application that runs the ActiNet and Accelerometer accelerometer processing pipelines without requiring the user to install Python, Conda, Java, or any developer tools.

## For users

1. Install using `WAVES_Processor_Setup_v1.0.0.exe`.
2. Open **WAVES Processor** from the desktop shortcut.
3. Select the folder containing `.gt3x` files.
4. Select or confirm the output folder.
5. Check **ActiNet**, **Accelerometer**, or both.
6. Click **Run Selected Processes**.
7. Wait. Open the output folder when done.

Outputs go to `Documents\WAVES Outputs\<timestamp>\` by default.
Logs go to `%LOCALAPPDATA%\WAVES Processor\logs\`.

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

Creates the `WAVES_actinet` and `WAVES_accelerometer` Conda environments.

### Step 2 — Smoke-test environments

```cmd
build_scripts\2_test_envs.bat
```

Checks that `actinet --help`, `accProcess --help`, and `java -version` all respond.

**Also test on a real file before packing:**

```cmd
conda activate WAVES_actinet
actinet C:\path\to\test.gt3x -o C:\path\to\test_outputs\actinet -p -c willetts

conda activate WAVES_accelerometer
accProcess C:\path\to\test_data -o C:\path\to\test_outputs\accelerometer --activityModel willetts --fileExtensions gt3x
```

Confirm output files appear. **Do not proceed to Step 3 until this passes.**

### Step 3 — Pack environments

```cmd
build_scripts\3_pack_envs.bat
```

Packs and unpacks both environments into `release_build\WAVES Processor\envs\`.
Runs `conda-unpack` to fix hardcoded paths.

**Critical test after this step** — run the unpacked exes directly without any conda activation:

```cmd
"release_build\WAVES Processor\envs\WAVES_actinet\Scripts\actinet.exe" --help
"release_build\WAVES Processor\envs\WAVES_accelerometer\Scripts\accProcess.exe" --help
```

Both must respond. If they fail here, the app will fail on the user's machine.

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

Compiles `installer\waves_processor.iss` and creates `installer\Output\WAVES_Processor_Setup_v1.0.0.exe`.

---

## Running tests

```cmd
python -m pytest tests/ -v
```

Tests cover validation logic and command construction (no external processes executed).

---

## Project structure

```
App/
├── config.json                  App configuration — versions, paths, flags
├── build_envs/
│   ├── actinet_environment.yml
│   └── accelerometer_environment.yml
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

1. Edit `build_envs\actinet_environment.yml` or `accelerometer_environment.yml`.
2. Edit `config.json` to match the new version strings.
3. Bump `app_version` in `config.json`.
4. Delete the old Conda environment and rebuild from Step 1.
5. Re-test, re-pack, re-build, re-install.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `actinet.exe` not found at startup | Environments not packed into `envs\` | Run Steps 3–4 |
| `java.exe` not found error in log | JAVA_HOME not set or JRE missing from packed env | Check `envs\WAVES_actinet\Library\jre\` exists |
| App works in dev, fails installed | `conda-unpack` did not run | Reinstall — the Inno Setup script runs it automatically |
| `accProcess` takes wrong arguments | CLI changed in the pinned version | Run `accProcess --help` and compare to `config.json` |
| Antivirus flags the exe | PyInstaller false positive | Expected — inform IT or consider code signing |

---

## Versions

| App version | actinet | accelerometer |
|---|---|---|
| 1.0.0 | 0.7.0 | 7.2.3 |
