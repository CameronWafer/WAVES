# Changelog

## v1.4 — Current release

- Step Count (stepcount 3.18.2) added as a third pipeline
- SSL and Random Forest model selection in GUI
- Supports `.cwa` files (Axivity) for Step Count in addition to `.gt3x` and `.bin`
- `WAVES_stepcount` bundled Conda environment added
- Invocation strategy changed to `python.exe -m <module>` for all pipelines — eliminates
  the conda-unpack path-fixup requirement on clean machines (see `HANDOFF.md`)
- `conda-unpack` step removed from installer

## v1.3

> Exact changes not fully documented. Known to be an intermediate release between v1.2 and v1.4.

## v1.2

- ActiNet 0.7.0 bundled environment
- Accelerometer 7.2.3 bundled environment
- tkinter GUI with folder selection, process checkboxes, status log
- Timestamped output subfolders
- Structured per-run log files
- Input validation with plain-English error messages
- Continues processing remaining files when one file fails
- Open Output Folder / Open Log Folder buttons
- Inno Setup installer targeting `%LOCALAPPDATA%` (no admin required)
