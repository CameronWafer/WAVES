"""
waves_validation.py
Input validation before any processing begins.
Returns plain-English error strings so the GUI can display them directly.
Returns None when validation passes.
"""

from pathlib import Path


def check_input_folder(input_dir: Path) -> str | None:
    if not input_dir.exists():
        return (
            f"The input folder does not exist:\n{input_dir}\n\n"
            "Please select a valid folder."
        )
    if not input_dir.is_dir():
        return (
            f"The selected path is not a folder:\n{input_dir}\n\n"
            "Please select a folder, not a file."
        )
    try:
        list(input_dir.iterdir())
    except PermissionError:
        return (
            f"Cannot read the input folder (permission denied):\n{input_dir}\n\n"
            "Try a different folder or check folder permissions."
        )
    return None


def check_output_folder(output_dir: Path) -> str | None:
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        return (
            f"Cannot create the output folder (permission denied):\n{output_dir}\n\n"
            "Try a different output location."
        )
    except Exception as e:
        return f"Cannot create the output folder:\n{output_dir}\n\n{e}"
    return None


def check_executable(exe_path: Path, tool_name: str) -> str | None:
    if not exe_path.exists():
        return (
            f"WAVES Processor is missing a required file for {tool_name}.\n\n"
            f"Missing:\n{exe_path}\n\n"
            "Please reinstall WAVES Processor or contact Cameron."
        )
    return None


def find_input_files(input_dir: Path, extensions: list[str]) -> list[Path]:
    """Return all files in input_dir matching the given extensions (with or without leading dot)."""
    found = []
    for ext in extensions:
        ext_clean = ext.lstrip(".")
        found.extend(sorted(input_dir.glob(f"*.{ext_clean}")))
    return found


def check_input_files(input_dir: Path, extensions: list[str], tool_name: str) -> str | None:
    files = find_input_files(input_dir, extensions)
    if not files:
        ext_display = ", ".join(f".{e.lstrip('.')}" for e in extensions)
        return (
            f"No {ext_display} files were found in the selected input folder.\n\n"
            f"Please select the folder that contains the raw {tool_name} files."
        )
    return None


def validate_for_actinet(input_dir: Path, actinet_exe: Path, config: dict) -> list[str]:
    errors = []
    err = check_input_folder(input_dir)
    if err:
        errors.append(err)
    else:
        err = check_input_files(input_dir, config["actinet"]["input_extensions"], "ActiGraph .gt3x")
        if err:
            errors.append(err)
    err = check_executable(actinet_exe, "ActiNet")
    if err:
        errors.append(err)
    return errors


def validate_for_accelerometer(input_dir: Path, accel_exe: Path, config: dict) -> list[str]:
    errors = []
    err = check_input_folder(input_dir)
    if err:
        errors.append(err)
    else:
        err = check_input_files(
            input_dir, config["accelerometer"]["input_extensions"], "accelerometer"
        )
        if err:
            errors.append(err)
    err = check_executable(accel_exe, "Accelerometer")
    if err:
        errors.append(err)
    return errors


def count_files(input_dir: Path, extensions: list[str]) -> int:
    return len(find_input_files(input_dir, extensions))
