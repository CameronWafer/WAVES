"""
waves_gui.py
Main GUI for WAVES Processor. Entry point for PyInstaller.

Architecture:
- Main thread: tkinter event loop only.
- Worker thread: all subprocess execution.
- Communication: queue polled every 100 ms via root.after().
  (tkinter is not thread-safe — never update widgets from the worker thread.)
"""

import queue
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

# ---------------------------------------------------------------------------
# Path bootstrap — must happen before local imports so src/ is on sys.path
# ---------------------------------------------------------------------------
import os
sys.path.insert(0, os.path.dirname(__file__))

from waves_config import (
    get_app_dir,
    get_actinet_env_dir,
    get_actinet_exe,
    get_accelerometer_env_dir,
    get_accelerometer_exe,
    get_default_output_dir,
    get_log_dir,
    load_config,
)
from waves_logging import WavesLogger
from waves_runner import run_actinet, run_accelerometer
from waves_validation import (
    check_output_folder,
    count_files,
    validate_for_actinet,
    validate_for_accelerometer,
)

APP_DIR = get_app_dir()

# ---------------------------------------------------------------------------
# Colours and fonts
# ---------------------------------------------------------------------------
CLR_BG = "#f4f4f4"
CLR_HEADER = "#1a3a5c"
CLR_BTN = "#1a3a5c"
CLR_BTN_FG = "white"
CLR_TERMINAL_BG = "#1e1e1e"
CLR_TERMINAL_FG = "#d4d4d4"
FONT_UI = ("Segoe UI", 9)
FONT_BOLD = ("Segoe UI", 9, "bold")
FONT_HEADER = ("Segoe UI", 13, "bold")
FONT_MONO = ("Consolas", 8)


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------

class WavesApp(tk.Tk):
    def __init__(self):
        super().__init__()

        try:
            self.cfg = load_config()
        except Exception as exc:
            messagebox.showerror(
                "Configuration Error",
                f"Could not load config.json.\n\nDetail: {exc}\n\n"
                "Please reinstall WAVES Processor or contact Cameron.",
            )
            self.destroy()
            return

        self.title(f"WAVES Processor  v{self.cfg.get('app_version', '')}")
        self.configure(bg=CLR_BG)
        self.resizable(False, False)

        self._queue: queue.Queue = queue.Queue()
        self._running = False
        self._last_output_dir: Path | None = None
        self._last_log_dir: Path | None = None

        self._build_ui()
        self._run_startup_checks()
        self._poll_queue()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        # Header bar
        header = tk.Frame(self, bg=CLR_HEADER, pady=10)
        header.grid(row=0, column=0, sticky="ew")
        tk.Label(
            header,
            text=f"WAVES Processor   v{self.cfg.get('app_version', '')}",
            bg=CLR_HEADER,
            fg="white",
            font=FONT_HEADER,
        ).pack()

        # Main body
        body = tk.Frame(self, bg=CLR_BG, padx=18, pady=14)
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)

        row = 0

        # --- Input folder ---
        tk.Label(body, text="Input folder:", bg=CLR_BG, font=FONT_BOLD).grid(
            row=row, column=0, sticky="w"
        )
        row += 1

        input_frame = tk.Frame(body, bg=CLR_BG)
        input_frame.grid(row=row, column=0, sticky="ew", pady=(2, 0))
        input_frame.columnconfigure(0, weight=1)

        self._input_var = tk.StringVar()
        tk.Entry(input_frame, textvariable=self._input_var, width=54, font=FONT_UI).grid(
            row=0, column=0, sticky="ew"
        )
        tk.Button(
            input_frame, text="Browse", command=self._browse_input, width=8
        ).grid(row=0, column=1, padx=(6, 0))
        row += 1

        # --- Output folder ---
        tk.Label(body, text="Output folder:", bg=CLR_BG, font=FONT_BOLD).grid(
            row=row, column=0, sticky="w", pady=(10, 0)
        )
        row += 1

        out_frame = tk.Frame(body, bg=CLR_BG)
        out_frame.grid(row=row, column=0, sticky="ew", pady=(2, 0))
        out_frame.columnconfigure(0, weight=1)

        self._output_var = tk.StringVar(value=str(get_default_output_dir(self.cfg)))
        tk.Entry(out_frame, textvariable=self._output_var, width=54, font=FONT_UI).grid(
            row=0, column=0, sticky="ew"
        )
        tk.Button(
            out_frame, text="Browse", command=self._browse_output, width=8
        ).grid(row=0, column=1, padx=(6, 0))
        row += 1

        # --- Process selection ---
        tk.Label(body, text="Processes:", bg=CLR_BG, font=FONT_BOLD).grid(
            row=row, column=0, sticky="w", pady=(12, 0)
        )
        row += 1

        checks = tk.Frame(body, bg=CLR_BG)
        checks.grid(row=row, column=0, sticky="w", pady=(2, 0))
        self._actinet_var = tk.BooleanVar(value=True)
        self._accel_var = tk.BooleanVar(value=False)
        tk.Checkbutton(
            checks, text="ActiNet", variable=self._actinet_var, bg=CLR_BG, font=FONT_UI
        ).pack(side="left", padx=(0, 20))
        tk.Checkbutton(
            checks, text="Accelerometer", variable=self._accel_var, bg=CLR_BG, font=FONT_UI
        ).pack(side="left")
        row += 1

        # --- Timestamp option ---
        self._timestamp_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            body,
            text="Create timestamped output subfolder  (recommended)",
            variable=self._timestamp_var,
            bg=CLR_BG,
            font=FONT_UI,
        ).grid(row=row, column=0, sticky="w", pady=(6, 0))
        row += 1

        # --- Run button ---
        self._run_btn = tk.Button(
            body,
            text="Run Selected Processes",
            command=self._on_run,
            bg=CLR_BTN,
            fg=CLR_BTN_FG,
            font=FONT_BOLD,
            relief="flat",
            padx=10,
            pady=7,
            cursor="hand2",
            activebackground="#245580",
            activeforeground="white",
        )
        self._run_btn.grid(row=row, column=0, sticky="ew", pady=(14, 6))
        row += 1

        # --- Status label ---
        tk.Label(body, text="Status:", bg=CLR_BG, font=FONT_BOLD).grid(
            row=row, column=0, sticky="w"
        )
        row += 1

        self._status_box = scrolledtext.ScrolledText(
            body,
            width=62,
            height=15,
            state="disabled",
            font=FONT_MONO,
            bg=CLR_TERMINAL_BG,
            fg=CLR_TERMINAL_FG,
            relief="flat",
            wrap="word",
        )
        self._status_box.grid(row=row, column=0, pady=(2, 8))
        row += 1

        # --- Bottom action buttons ---
        bottom = tk.Frame(body, bg=CLR_BG)
        bottom.grid(row=row, column=0, sticky="w")

        self._open_output_btn = tk.Button(
            bottom,
            text="Open Output Folder",
            command=self._open_output_folder,
            state="disabled",
        )
        self._open_output_btn.pack(side="left", padx=(0, 8))

        self._open_log_btn = tk.Button(
            bottom,
            text="Open Log Folder",
            command=self._open_log_folder,
            state="disabled",
        )
        self._open_log_btn.pack(side="left")

    # ------------------------------------------------------------------
    # Folder browsing
    # ------------------------------------------------------------------

    def _browse_input(self):
        d = filedialog.askdirectory(title="Select folder containing .gt3x files")
        if d:
            self._input_var.set(d)

    def _browse_output(self):
        d = filedialog.askdirectory(title="Select output folder")
        if d:
            self._output_var.set(d)

    # ------------------------------------------------------------------
    # Status box helpers
    # ------------------------------------------------------------------

    def _log(self, msg: str):
        self._status_box.configure(state="normal")
        self._status_box.insert(tk.END, msg + "\n")
        self._status_box.see(tk.END)
        self._status_box.configure(state="disabled")

    def _clear_log(self):
        self._status_box.configure(state="normal")
        self._status_box.delete("1.0", tk.END)
        self._status_box.configure(state="disabled")

    # ------------------------------------------------------------------
    # Startup checks
    # ------------------------------------------------------------------

    def _run_startup_checks(self):
        missing = []
        actinet_exe = get_actinet_exe(self.cfg, APP_DIR)
        accel_exe = get_accelerometer_exe(self.cfg, APP_DIR)
        if not actinet_exe.exists():
            missing.append(str(actinet_exe))
        if not accel_exe.exists():
            missing.append(str(accel_exe))

        if missing:
            lines = "\n".join(missing)
            messagebox.showwarning(
                "Missing Files",
                f"WAVES Processor is missing required files.\n\nMissing:\n{lines}\n\n"
                "Please reinstall WAVES Processor or contact Cameron.",
            )
        else:
            self._log("Ready. Select a folder and choose a process to run.")

    # ------------------------------------------------------------------
    # Run button handler
    # ------------------------------------------------------------------

    def _on_run(self):
        if self._running:
            return

        input_str = self._input_var.get().strip()
        output_str = self._output_var.get().strip()

        if not input_str:
            messagebox.showwarning("Input Required", "Please select an input folder.")
            return
        if not output_str:
            messagebox.showwarning("Output Required", "Please select an output folder.")
            return

        run_actinet_flag = self._actinet_var.get()
        run_accel_flag = self._accel_var.get()

        if not run_actinet_flag and not run_accel_flag:
            messagebox.showwarning(
                "No Process Selected", "Please select at least one process to run."
            )
            return

        input_dir = Path(input_str)
        output_base = Path(output_str)
        output_dir = (
            output_base / datetime.now().strftime("%Y-%m-%d_%H%M")
            if self._timestamp_var.get()
            else output_base
        )

        self._clear_log()
        self._last_output_dir = None
        self._last_log_dir = None
        self._run_btn.configure(state="disabled")
        self._open_output_btn.configure(state="disabled")
        self._open_log_btn.configure(state="disabled")
        self._running = True

        threading.Thread(
            target=self._worker,
            args=(input_dir, output_dir, run_actinet_flag, run_accel_flag),
            daemon=True,
        ).start()

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _push(self, msg: str):
        """Send a log message from the worker thread to the GUI queue."""
        self._queue.put(("log", msg))

    def _worker(
        self,
        input_dir: Path,
        output_dir: Path,
        run_actinet_flag: bool,
        run_accel_flag: bool,
    ):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        log_dir = get_log_dir(self.cfg, APP_DIR)
        log = WavesLogger(log_dir, timestamp, self.cfg)

        selected = []
        if run_actinet_flag:
            selected.append("ActiNet")
        if run_accel_flag:
            selected.append("Accelerometer")

        log.write_header(input_dir, output_dir, selected)

        actinet_exe = get_actinet_exe(self.cfg, APP_DIR)
        accel_exe = get_accelerometer_exe(self.cfg, APP_DIR)
        actinet_env_dir = get_actinet_env_dir(self.cfg, APP_DIR)
        accel_env_dir = get_accelerometer_env_dir(self.cfg, APP_DIR)

        # --- Pre-run validation ---
        self._push("Validating inputs...")
        errors: list[str] = []

        if run_actinet_flag:
            errors += validate_for_actinet(input_dir, actinet_exe, self.cfg)
        if run_accel_flag:
            errors += validate_for_accelerometer(input_dir, accel_exe, self.cfg)

        out_err = check_output_folder(output_dir)
        if out_err:
            errors.append(out_err)

        if errors:
            self._queue.put(("validation_error", "\n\n─────────────────────\n\n".join(errors)))
            self._queue.put(("done", None))
            return

        if run_actinet_flag:
            n = count_files(input_dir, self.cfg["actinet"]["input_extensions"])
            self._push(f"Found {n} .gt3x file(s).")
            est_min = n * 12 if run_actinet_flag else 0
            if run_accel_flag:
                est_min += n * 25
            self._push(f"Estimated time: ~{est_min}–{est_min + n * 5} minutes. Please be patient.")

        # --- ActiNet ---
        actinet_results = None
        if run_actinet_flag:
            self._push("─" * 40)
            self._push("Starting ActiNet...")
            actinet_results = run_actinet(
                input_dir=input_dir,
                output_dir=output_dir,
                env_dir=actinet_env_dir,
                actinet_exe=actinet_exe,
                config=self.cfg,
                log=log,
                progress_callback=self._push,
            )

        # --- Accelerometer ---
        accel_results = None
        if run_accel_flag:
            self._push("─" * 40)
            self._push("Starting Accelerometer...")
            accel_results = run_accelerometer(
                input_dir=input_dir,
                output_dir=output_dir,
                env_dir=accel_env_dir,
                accprocess_exe=accel_exe,
                config=self.cfg,
                log=log,
                progress_callback=self._push,
            )

        log.write_summary(actinet_results=actinet_results, accelerometer_results=accel_results)

        # --- Final summary in GUI ---
        self._push("─" * 40)
        self._push("Run complete.")
        self._push("")

        for label, results in [("ActiNet", actinet_results), ("Accelerometer", accel_results)]:
            if results is None:
                continue
            ok = [r for r in results if r["status"] == "success"]
            bad = [r for r in results if r["status"] == "failed"]
            self._push(f"{label}   —   Succeeded: {len(ok)}   Failed: {len(bad)}")
            for r in bad:
                self._push(f"  FAILED: {Path(r['file']).name}")

        self._push("")
        self._push(f"Outputs:  {output_dir}")
        self._push(f"Log:      {log.log_path}")

        self._last_output_dir = output_dir
        self._last_log_dir = log_dir
        self._queue.put(("done", None))

    # ------------------------------------------------------------------
    # Queue polling (main thread)
    # ------------------------------------------------------------------

    def _poll_queue(self):
        try:
            while True:
                msg_type, payload = self._queue.get_nowait()
                if msg_type == "log":
                    self._log(payload)
                elif msg_type == "validation_error":
                    messagebox.showwarning("Cannot Run", payload)
                    self._log("Validation failed — see the message above.")
                elif msg_type == "done":
                    self._running = False
                    self._run_btn.configure(state="normal")
                    if self._last_output_dir:
                        self._open_output_btn.configure(state="normal")
                    if self._last_log_dir:
                        self._open_log_btn.configure(state="normal")
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)

    # ------------------------------------------------------------------
    # Open folder buttons
    # ------------------------------------------------------------------

    def _open_output_folder(self):
        if self._last_output_dir and self._last_output_dir.exists():
            subprocess.Popen(["explorer", str(self._last_output_dir)])

    def _open_log_folder(self):
        if self._last_log_dir and self._last_log_dir.exists():
            subprocess.Popen(["explorer", str(self._last_log_dir)])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    app = WavesApp()
    app.mainloop()


if __name__ == "__main__":
    main()
