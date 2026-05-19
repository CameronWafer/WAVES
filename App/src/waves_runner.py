"""
waves_runner.py
Executes the ActiNet and Accelerometer pipelines using bundled executables.

Confirmed working commands (tested 2026-05-18):
  actinet.exe "file.gt3x" -o "output_dir" -p
  accProcess.exe "file.gt3x" -o "output_dir"

Key design decisions:
- Sets PATH and JAVA_HOME explicitly so bundled Java is found on clean machines.
- Uses list-form subprocess.run() to handle paths with spaces.
- BOTH pipelines process files one-by-one (accProcess takes a single file, not a folder).
- Continues on single-file failures — all files are attempted.
- Approximate processing times: actinet ~12 min/file, accProcess ~25 min/file.
"""

import os
import subprocess
from pathlib import Path
from typing import Callable, Optional

from waves_logging import WavesLogger


def _build_subprocess_env(env_dir: Path) -> dict:
    """
    Return a copy of os.environ with the bundled Conda env's bin paths prepended
    and JAVA_HOME pointing at the bundled JRE.

    Required on clean machines where neither Python nor Java is installed
    system-wide. Without this, actinet/accProcess cannot locate java.exe at runtime.
    """
    env = os.environ.copy()

    path_prepend = os.pathsep.join([
        str(env_dir / "Library" / "bin"),
        str(env_dir / "Library" / "usr" / "bin"),
        str(env_dir / "Library" / "mingw-w64" / "bin"),
        str(env_dir / "Scripts"),
        str(env_dir),
    ])
    env["PATH"] = path_prepend + os.pathsep + env.get("PATH", "")

    # Find bundled JRE and set JAVA_HOME
    jre_candidates = [
        env_dir / "Library" / "jre",
        env_dir / "Library" / "java" / "jre",
        env_dir / "Library",
    ]
    for candidate in jre_candidates:
        if candidate.exists() and (candidate / "bin" / "java.exe").exists():
            env["JAVA_HOME"] = str(candidate)
            break

    return env


def _collect_files(input_dir: Path, extensions: list[str]) -> list[Path]:
    files: list[Path] = []
    for ext in extensions:
        files.extend(sorted(input_dir.glob(f"*.{ext.lstrip('.')}")))
    return files


def _run_one_file(
    command: list[str],
    file: Path,
    subprocess_env: dict,
    log: WavesLogger,
) -> dict:
    """Run a single subprocess command and return a result dict."""
    log.write_command(command)
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            env=subprocess_env,
        )
        log.write_stdout(completed.stdout)
        log.write_stderr(completed.stderr)
        return {"file": str(file), "status": "success"}

    except subprocess.CalledProcessError as error:
        log.write_error(error)
        log.write_stdout(error.stdout or "")
        log.write_stderr(error.stderr or "")
        return {"file": str(file), "status": "failed", "return_code": error.returncode}

    except Exception as exc:
        log.write_exception(f"Unexpected error on {file.name}", exc)
        return {"file": str(file), "status": "failed", "return_code": -1}


# ---------------------------------------------------------------------------
# ActiNet runner
# ---------------------------------------------------------------------------

def run_actinet(
    input_dir: Path,
    output_dir: Path,
    env_dir: Path,
    actinet_exe: Path,
    config: dict,
    log: WavesLogger,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> list[dict]:
    """
    Process each input file individually through actinet.
    Returns a list of per-file result dicts.
    Continues on single-file failures.

    Confirmed command: actinet.exe "file.gt3x" -o "output_dir" -p
    actinet uses its bundled torch_hub_cache automatically — no -c flag needed.
    """
    actinet_output_dir = output_dir / "actinet"
    actinet_output_dir.mkdir(parents=True, exist_ok=True)

    make_plot = config["actinet"]["make_plot"]
    extensions = config["actinet"]["input_extensions"]
    files = _collect_files(input_dir, extensions)

    if not files:
        return [{"file": "", "status": "no_files"}]

    subprocess_env = _build_subprocess_env(env_dir)
    results: list[dict] = []

    for i, file in enumerate(files, start=1):
        msg = f"ActiNet: processing file {i} of {len(files)}: {file.name}"
        log.write_info(msg)
        if progress_callback:
            progress_callback(msg)

        command = [str(actinet_exe), str(file), "-o", str(actinet_output_dir)]
        if make_plot:
            command.append("-p")

        results.append(_run_one_file(command, file, subprocess_env, log))

    return results


# ---------------------------------------------------------------------------
# Accelerometer runner
# ---------------------------------------------------------------------------

def run_accelerometer(
    input_dir: Path,
    output_dir: Path,
    env_dir: Path,
    accprocess_exe: Path,
    config: dict,
    log: WavesLogger,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> list[dict]:
    """
    Process each input file individually through accProcess.
    accProcess takes a single file — there is no folder batch mode.
    Returns a list of per-file result dicts (same structure as run_actinet).
    Continues on single-file failures.

    Confirmed command: accProcess.exe "file.gt3x" -o "output_dir"
    Default walmsley model is used automatically — no --activityModel flag needed.
    """
    accelerometer_output_dir = output_dir / "accelerometer"
    accelerometer_output_dir.mkdir(parents=True, exist_ok=True)

    extensions = config["accelerometer"]["input_extensions"]
    files = _collect_files(input_dir, extensions)

    if not files:
        return [{"file": "", "status": "no_files"}]

    subprocess_env = _build_subprocess_env(env_dir)
    results: list[dict] = []

    for i, file in enumerate(files, start=1):
        msg = f"Accelerometer: processing file {i} of {len(files)}: {file.name}"
        log.write_info(msg)
        if progress_callback:
            progress_callback(msg)

        command = [str(accprocess_exe), str(file), "-o", str(accelerometer_output_dir)]
        results.append(_run_one_file(command, file, subprocess_env, log))

    return results
