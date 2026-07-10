"""
waves_logging.py
Structured run logger. Creates one timestamped .log file per run.
Writes headers, commands, stdout/stderr, per-file results, and summaries.
"""

import logging
import os
import socket
import traceback
from datetime import datetime
from pathlib import Path


class WavesLogger:
    def __init__(self, log_dir: Path, timestamp: str, config: dict):
        self.log_dir = log_dir
        self.timestamp = timestamp
        self.config = config
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = log_dir / f"{timestamp}_run.log"

        self._logger = logging.getLogger(f"waves_{timestamp}")
        self._logger.setLevel(logging.DEBUG)
        # Avoid duplicate handlers if logger is reused
        if not self._logger.handlers:
            handler = logging.FileHandler(self.log_path, encoding="utf-8")
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)

    def write_header(self, input_dir: Path, output_dir: Path, selected: list[str]):
        self._write("=" * 60)
        self._write("WAVES Processor Run Log")
        self._write("=" * 60)
        self._write(f"App version:    {self.config.get('app_version', '?')}")
        self._write(f"Run started:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._write(f"User:           {os.getenv('USERNAME', 'unknown')}")
        self._write(f"Computer:       {socket.gethostname()}")
        self._write(f"Input folder:   {input_dir}")
        self._write(f"Output folder:  {output_dir}")
        self._write(f"Selected:       {', '.join(selected)}")
        if "ActiNet" in selected:
            self._write(f"ActiNet target: {self.config['actinet']['version']}")
        if "Accelerometer" in selected:
            self._write(f"Accel target:   {self.config['accelerometer']['version']}")
        self._write("=" * 60)

    def write_command(self, command: list[str]):
        self._write("\nRunning command:")
        for part in command:
            self._write(f"  {part}")

    def write_stdout(self, text: str):
        if text and text.strip():
            self._write(f"\n--- stdout ---\n{text.rstrip()}")

    def write_stderr(self, text: str):
        if text and text.strip():
            self._write(f"\n--- stderr ---\n{text.rstrip()}")

    def write_error(self, error: Exception):
        self._write(f"\n--- error ---\n{error}")

    def write_exception(self, msg: str, exc: Exception):
        self._write(f"\n--- exception: {msg} ---")
        self._write(traceback.format_exc())

    def write_info(self, msg: str):
        self._write(msg)

    def write_summary(
        self,
        actinet_results: list[dict] | None = None,
        accelerometer_results: list[dict] | None = None,
        stepcount_results: list[dict] | None = None,
    ):
        self._write("\n" + "=" * 60)
        self._write("Run Summary")
        self._write("=" * 60)

        all_ok = True

        for label, results in [
            ("ActiNet", actinet_results),
            ("Accelerometer", accelerometer_results),
            ("Step Count", stepcount_results),
        ]:
            if results is None:
                continue
            succeeded = [r for r in results if r["status"] == "success"]
            failed = [r for r in results if r["status"] == "failed"]
            self._write(f"{label} files found:   {len(results)}")
            self._write(f"{label} succeeded:     {len(succeeded)}")
            self._write(f"{label} failed:        {len(failed)}")
            if failed:
                all_ok = False
                self._write(f"{label} failed files:")
                for r in failed:
                    code = r.get("return_code", "?")
                    self._write(f"  - {Path(r['file']).name}  (return code: {code})")

        self._write("\n" + ("Run completed successfully." if all_ok else "Run completed with failures."))
        self._write("=" * 60)

    def _write(self, msg: str):
        self._logger.info(msg)
