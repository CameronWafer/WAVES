# WAVES Processor — Agent Handoff Report
**Date:** 2026-05-27  
**Previous chat:** [WAVES App Build & Debug](db94c9c6-9e1b-479a-a6b2-e6a24b1fc2f3)

---

## What This Project Is

A Windows desktop app (`WAVES Processor`) that wraps two OxWearables command-line tools — **actinet** and **accProcess** — so a non-technical worker can process accelerometer files without Python, Conda, Java, or a terminal. The app bundles two packed Conda environments and calls the tool executables directly via `subprocess.run()`.

---

## Repository Location

```
C:\Users\HELIOS-300\Desktop\WAVES\App\
```

This is inside a git repo at `C:\Users\HELIOS-300\Desktop\WAVES\`. The `App/` folder has its own `.gitignore` that blocks `envs/`, `release_build/`, `dist/`, `build/`, `*.zip`, `*.spec`.

---

## Complete File Structure

```
App/
├── config.json                  <- runtime config, version = "1.2"
├── .gitignore
├── README.md
├── CHANGELOG.md
├── build_envs/
│   ├── actinet_environment.yml
│   └── accelerometer_environment.yml
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
│   └── waves_processor.iss      <- Inno Setup script, version = "1.2"
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
C:\Users\HELIOS-300\AppData\Local\r-miniconda\envs\WHO_WAVES_actinet
C:\Users\HELIOS-300\AppData\Local\r-miniconda\envs\WHO_WAVES_accelerometer
```

Both have been tested and confirmed working. Both have `conda-pack==0.9.1` installed via pip.

---

## Confirmed Working Commands (tested on dev machine)

```cmd
actinet.exe "file.gt3x" -o "output_dir" -p
accProcess.exe "file.gt3x" -o "output_dir"
```

Key facts confirmed by testing:
- No `-c` flag needed for actinet (bundled model at `actinet\torch_hub_cache\...`)
- No `--activityModel` flag needed for accProcess (default walmsley used)
- accProcess takes a **single file**, not a folder
- Both support `.gt3x` and `.bin` (GENEActiv) files
- Processing time: actinet ~12 min/file, accProcess ~25 min/file
- Paths with spaces and parentheses work fine

---

## Build Artifacts Location

```
D:\WAVES Processor\              <- complete working app folder (dev machine)
D:\WAVES Installer Output\       <- compiled installer output
C:\Users\HELIOS-300\Desktop\WAVES\App\release_build\   <- intermediate packed env zips (may be deleted)
```

---

## config.json (current)

```json
{
  "app_name": "WAVES Processor",
  "app_version": "1.2",
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
  "logging": {
    "log_folder": "logs"
  }
}
```

---

## GUI Features (all implemented in waves_gui.py)

- Input folder / Output folder browse
- ActiNet and Accelerometer checkboxes
- Timestamped output subfolder checkbox (default on)
- Spinning ttk.Progressbar while running
- Live elapsed time counter (updates every second)
- Final "Total time: HH:MM:SS" when done
- Status box (dark terminal-style, Consolas font)
- Open Output Folder button (enabled after run)
- Open Log Folder button (enabled after run)
- Help button (separate row) -> modal dialog with 5 Q&As
- Contact info in Help: "Cameron Hafer at cameroonhafer@gmail.com"
- No em dashes anywhere in user-visible text
- Run button disabled while processing
- Background thread for processing (queue-based GUI updates)

---

## Current Blocking Issue — CRITICAL

**The installer fails to fix hardcoded paths in the packed Conda environments on clean machines.**

### Root cause chain:

1. Conda environments are packed with `conda-pack` from their original location (`r-miniconda\envs\WHO_WAVES_actinet`). All Python launcher `.exe` files (including `actinet.exe`, `accProcess.exe`, `conda-unpack.exe`) have this path hardcoded.

2. On the dev machine, `conda-unpack.exe` ran successfully (because the original Python path existed there) and fixed all paths to `release_build\...\WAVES_actinet`.

3. After robocopy to `D:\WAVES Processor`, the paths now point to `release_build`. Then `conda-unpack-script.py` was run using `python.exe` to fix to `D:\WAVES Processor`.

4. Installer ships from `D:\WAVES Processor`.

5. On clean machine (user: Panda, `C:\Users\Panda\AppData\Local\WAVES Processor`), installer's `[Run]` section tries to run `conda-unpack-script.py` using `python.exe`. **This either silently fails or doesn't fix correctly**, because `actinet.exe` still fails with exit code 1 and no stderr output.

### Evidence from Panda machine:

First test (v1.1 old installer -- conda-unpack.exe approach):
```
Fatal error in launcher: Unable to create process using
'"\?\c:\users\helios-300\desktop\waves\app\release_build\waves processor\envs\waves_actinet\python.exe"
```
This confirmed conda-unpack never ran.

Second test (v1.1 new installer -- conda-unpack-script.py approach):
```
exit status 1, no stderr output
```
conda-unpack-script.py either ran and didn't fully fix things, or python.exe itself failed silently (runhidden hides all errors).

### What's in Scripts folder (no -script.py files):

```
D:\WAVES Processor\envs\WAVES_actinet\Scripts\
  actinet.exe          (108,371 bytes -- Python launcher)
  actinet-collate-outputs.exe

D:\WAVES Processor\envs\WAVES_accelerometer\Scripts\
  accProcess.exe       (108,386 bytes -- Python launcher)
```

---

## Fix Applied (2026-05-27) — RESOLVED

**Root cause bypassed by switching to `python.exe -m <module>` invocation.**

### What was discovered

Neither package has a `__main__.py`, so `-m actinet` and `-m accelerometer` both fail.
The correct submodule entry points from the dist-info metadata are:
- `actinet` -> `actinet.actinet:main`
- `accProcess` -> `accelerometer.accProcess:main`

Both confirmed working:
```cmd
"D:\WAVES Processor\envs\WAVES_actinet\python.exe" -m actinet.actinet --help   -> prints help OK
"D:\WAVES Processor\envs\WAVES_accelerometer\python.exe" -m accelerometer.accProcess --help   -> prints help OK
```

### Code changes made

1. `waves_config.py` — removed `get_actinet_exe` / `get_accelerometer_exe`; added `get_actinet_python()` and `get_accelerometer_python()` returning `env_dir / "python.exe"`
2. `waves_runner.py` — commands now `[python_exe, "-m", "actinet.actinet", file, "-o", outdir, ...]` and `[python_exe, "-m", "accelerometer.accProcess", file, "-o", outdir]`
3. `waves_validation.py` — `validate_for_actinet` / `validate_for_accelerometer` now accept `python_exe` (checks `python.exe` exists instead of `.exe` launcher)
4. `waves_gui.py` — imports updated, all call sites updated to use `actinet_python` / `accel_python`
5. `installer/waves_processor.iss` — conda-unpack `[Run]` entries removed; only the "launch after install" entry remains

### Next steps

1. **Rebuild the exe:**
```cmd
cd "C:\Users\HELIOS-300\Desktop\WAVES\App"
pyinstaller --windowed --name "WAVES Processor" src\waves_gui.py
robocopy "C:\Users\HELIOS-300\Desktop\WAVES\App\dist\WAVES Processor" "D:\WAVES Processor" /E /R:1 /W:1 /XD envs
copy /Y "C:\Users\HELIOS-300\Desktop\WAVES\App\config.json" "D:\WAVES Processor\"
```

2. **Test on dev machine** with a real `.gt3x` file before building the installer.

3. **Rebuild installer** (~30 min):
```cmd
"D:\Program Files (x86)\Inno Setup 6\ISCC.exe" "C:\Users\HELIOS-300\Desktop\WAVES\App\installer\waves_processor.iss"
```

4. **Test on Panda machine** (`DESKTOP-76F3AG8`).

---

## Installer Details

- Tool: Inno Setup 6 at `D:\Program Files (x86)\Inno Setup 6\ISCC.exe`
- Source: `D:\WAVES Processor\`
- Output: `D:\WAVES Installer Output\WAVES_Processor_Setup_v{version}.exe`
- Install target: `%LOCALAPPDATA%\WAVES Processor\` (no admin required)
- Compile command:
```cmd
"D:\Program Files (x86)\Inno Setup 6\ISCC.exe" "C:\Users\HELIOS-300\Desktop\WAVES\App\installer\waves_processor.iss"
```
- Compile time: ~30 minutes (large environments)
- Creates desktop shortcut automatically

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

---

## Test Machine Details

- Dev machine: `HELIOS-300`, Windows 10/11, Anaconda3 + r-miniconda
- Clean test machine: user `Panda`, computer `DESKTOP-76F3AG8`
- Test .gt3x file: `C:\Users\HELIOS-300\Desktop\Data\ACT24_104_WRT (2019-07-24).gt3x`
