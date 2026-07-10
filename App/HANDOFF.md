# WAVES Processor — Agent Handoff Report

**Last updated:** 2026-07-09
**Previous chat:** [WAVES App Build & Debug](db94c9c6-9e1b-479a-a6b2-e6a24b1fc2f3)

---

## What This Project Is

A Windows desktop app (`WAVES Processor`) that wraps three OxWearables command-line tools —
**actinet**, **accProcess**, and **stepcount** — so a non-technical worker can process
accelerometer files without Python, Conda, Java, or a terminal. The app bundles three packed
Conda environments and calls each tool via `python.exe -m <module>` using `subprocess.run()`.

Current version: **1.4**

---

## Repository Location

```
C:\Users\HELIOS-300\Desktop\WAVES\App\
```

Inside a git repo at `C:\Users\HELIOS-300\Desktop\WAVES\`. The `App/` folder has its own
`.gitignore` that blocks `envs/`, `release_build/`, `dist/`, `build/`, `*.zip`.

---

## Complete File Structure

```
App/
├── config.json                  <- runtime config, version = "1.4"
├── .gitignore
├── README.md
├── CHANGELOG.md
├── HANDOFF.md
├── build_envs/
│   ├── actinet_environment.yml
│   ├── accelerometer_environment.yml
│   └── stepcount_environment.yml
├── src/
│   ├── waves_gui.py             <- PyInstaller entry point
│   ├── waves_runner.py          <- subprocess execution
│   ├── waves_config.py          <- path resolution
│   ├── waves_logging.py         <- structured run logger
│   └── waves_validation.py      <- pre-run validation
├── build_scripts/
│   ├── 1_build_envs.bat
│   ├── 2_test_envs.bat
│   ├── 3_pack_envs.bat
│   ├── 4_build_app.bat
│   └── 5_build_installer.bat
├── installer/
│   └── waves_processor.iss      <- Inno Setup script, version = "1.4"
├── tests/
│   ├── test_validation.py
│   └── test_command_building.py
└── resources/
    └── .gitkeep
```

---

## Source Conda Environments (on dev machine)

Located in r-miniconda (separate from main Anaconda3):

```
C:\Users\HELIOS-300\AppData\Local\r-miniconda\envs\WAVES_actinet
C:\Users\HELIOS-300\AppData\Local\r-miniconda\envs\WAVES_accelerometer
C:\Users\HELIOS-300\AppData\Local\r-miniconda\envs\WAVES_stepcount
```

---

## Confirmed Working Commands

The app uses `python.exe -m <module>` for all three pipelines:

```cmd
python.exe -m actinet.actinet "file.gt3x" -o "output_dir" -p
python.exe -m accelerometer.accProcess "file.gt3x" -o "output_dir"
python.exe -m stepcount.stepcount "file.gt3x" -o "output_dir"
python.exe -m stepcount.stepcount "file.gt3x" -o "output_dir" -t rf   (Random Forest model)
```

Key facts:
- No `-c` flag needed for actinet (bundled model at `actinet\torch_hub_cache\...`)
- No `--activityModel` flag needed for accProcess (default walmsley used)
- All three take a **single file**, not a folder — runner iterates files one-by-one
- actinet and accProcess support `.gt3x` and `.bin`; stepcount also supports `.cwa`
- Processing time: actinet ~12 min/file, accProcess ~25 min/file, stepcount varies

---

## Build Artifacts Location

```
D:\WAVES Processor\              <- complete working app folder (dev machine)
D:\WAVES Installer Output\       <- compiled installer output
C:\Users\HELIOS-300\Desktop\WAVES\App\release_build\   <- intermediate packed env zips
```

---

## config.json (current)

```json
{
  "app_name": "WAVES Processor",
  "app_version": "1.4",
  "actinet": {
    "enabled": true,
    "env_name": "WAVES_actinet",
    "executable": "envs/WAVES_actinet/Scripts/actinet.exe",
    "version": "0.7.0",
    "input_extensions": [".gt3x", ".bin"],
    "make_plot": true
  },
  "accelerometer": {
    "enabled": true,
    "env_name": "WAVES_accelerometer",
    "executable": "envs/WAVES_accelerometer/Scripts/accProcess.exe",
    "version": "7.2.3",
    "input_extensions": [".gt3x", ".bin"],
    "recursive": false
  },
  "outputs": {
    "default_folder_name": "WAVES Outputs",
    "timestamp_run_folders": true
  },
  "stepcount": {
    "enabled": true,
    "env_name": "WAVES_stepcount",
    "version": "3.18.2",
    "input_extensions": [".gt3x", ".bin", ".cwa"]
  },
  "logging": {
    "log_folder": "logs"
  }
}
```

---

## GUI Features (waves_gui.py)

- Input folder / Output folder browse
- ActiNet, Accelerometer, Step Count (SSL), Step Count (RF) checkboxes
- Timestamped output subfolder checkbox (default on)
- Spinning ttk.Progressbar while running
- Live elapsed time counter (updates every second)
- Final "Total time: HH:MM:SS" when done
- Status box (dark terminal-style, Consolas font)
- Open Output Folder button (enabled after run)
- Open Log Folder button (enabled after run)
- Help button -> modal dialog with Q&As
- Contact info in Help: "Cameron Hafer at cameroonhafer@gmail.com"
- Run button disabled while processing
- Background thread for processing (queue-based GUI updates)

---

## Key Design Decision: python.exe -m invocation

**Background (resolved 2026-05-27):** The original app called `actinet.exe` / `accProcess.exe`
launcher scripts directly. These are Python launcher `.exe` files with hardcoded install paths
baked in at pack time. On clean machines, `conda-unpack` was supposed to re-stamp these paths,
but it silently failed — the launchers had the dev machine path and would not run elsewhere.

**Fix:** All pipelines now call `python.exe -m <module>` instead:

```
python.exe -m actinet.actinet
python.exe -m accelerometer.accProcess
python.exe -m stepcount.stepcount
```

`python.exe` is a native binary with no hardcoded paths, so it works on any machine without any
post-install path fixup. The `conda-unpack` step was removed from the installer.

Entry points confirmed from dist-info metadata:
- `actinet` -> `actinet.actinet:main`
- `accProcess` -> `accelerometer.accProcess:main`
- `stepcount` -> `stepcount.stepcount:main`

Code that implements this: `waves_config.py` (`get_actinet_python`, `get_accelerometer_python`,
`get_stepcount_python`) and `waves_runner.py` (`run_actinet`, `run_accelerometer`, `run_stepcount`).

---

## Rebuild Commands (after any source change)

```cmd
cd "C:\Users\HELIOS-300\Desktop\WAVES\App"
pyinstaller --windowed --name "WAVES Processor" src\waves_gui.py
robocopy "C:\Users\HELIOS-300\Desktop\WAVES\App\dist\WAVES Processor" "D:\WAVES Processor" /E /R:1 /W:1 /XD envs
copy /Y "C:\Users\HELIOS-300\Desktop\WAVES\App\config.json" "D:\WAVES Processor\"
```

Then if installer rebuild needed:

```cmd
"D:\Program Files (x86)\Inno Setup 6\ISCC.exe" "C:\Users\HELIOS-300\Desktop\WAVES\App\installer\waves_processor.iss"
```

Installer compile time: ~30 minutes (large environments).

---

## Installer Details

- Tool: Inno Setup 6 at `D:\Program Files (x86)\Inno Setup 6\ISCC.exe`
- Source: `D:\WAVES Processor\`
- Output: `D:\WAVES Installer Output\WAVES_Processor_Setup_v{version}.exe`
- Install target: `%LOCALAPPDATA%\WAVES Processor\` (no admin required)
- Creates desktop shortcut automatically
- No `conda-unpack` step in installer (removed in v1.4)

---

## Test Machine Details

- Dev machine: `HELIOS-300`, Windows 10/11, Anaconda3 + r-miniconda
- Clean test machine: user `Panda`, computer `DESKTOP-76F3AG8`
- Test file: `C:\Users\HELIOS-300\Desktop\Data\ACT24_104_WRT (2019-07-24).gt3x`
