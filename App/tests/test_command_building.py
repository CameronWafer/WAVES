"""
test_command_building.py
Tests that the runner builds correct subprocess commands without actually
executing any external processes. Uses monkeypatching.
"""

import sys
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
import tempfile
import shutil

from waves_runner import run_actinet, run_accelerometer


FAKE_CONFIG = {
    "actinet": {
        "input_extensions": [".gt3x"],
        "make_plot": True,
    },
    "accelerometer": {
        "input_extensions": [".gt3x"],
    },
}


@pytest.fixture
def dirs():
    base = Path(tempfile.mkdtemp())
    input_dir = base / "input"
    output_dir = base / "output"
    env_dir = base / "env"
    input_dir.mkdir()
    output_dir.mkdir()
    env_dir.mkdir()
    yield input_dir, output_dir, env_dir
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture
def fake_log():
    log = MagicMock()
    return log


# ---------------------------------------------------------------------------
# ActiNet command construction
# ---------------------------------------------------------------------------

class TestActiNetCommands:
    def test_processes_each_gt3x_file(self, dirs, fake_log):
        input_dir, output_dir, env_dir = dirs
        (input_dir / "p001.gt3x").write_text("")
        (input_dir / "p002.gt3x").write_text("")

        fake_exe = env_dir / "actinet.exe"
        fake_exe.write_text("")

        captured_commands = []

        def fake_run(command, **kwargs):
            captured_commands.append(command)
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            result.returncode = 0
            return result

        with patch("waves_runner.subprocess.run", side_effect=fake_run):
            results = run_actinet(
                input_dir=input_dir,
                output_dir=output_dir,
                env_dir=env_dir,
                actinet_exe=fake_exe,
                config=FAKE_CONFIG,
                log=fake_log,
            )

        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)
        assert len(captured_commands) == 2

    def test_command_includes_classifier_and_plot(self, dirs, fake_log):
        input_dir, output_dir, env_dir = dirs
        (input_dir / "p001.gt3x").write_text("")
        fake_exe = env_dir / "actinet.exe"
        fake_exe.write_text("")

        captured = []

        def fake_run(command, **kwargs):
            captured.append(command)
            r = MagicMock()
            r.stdout = r.stderr = ""
            return r

        with patch("waves_runner.subprocess.run", side_effect=fake_run):
            run_actinet(input_dir, output_dir, env_dir, fake_exe, FAKE_CONFIG, fake_log)

        cmd = captured[0]
        assert "-c" in cmd
        assert "willetts" in cmd
        assert "-p" in cmd

    def test_continues_after_one_file_fails(self, dirs, fake_log):
        input_dir, output_dir, env_dir = dirs
        (input_dir / "good.gt3x").write_text("")
        (input_dir / "bad.gt3x").write_text("")
        fake_exe = env_dir / "actinet.exe"
        fake_exe.write_text("")

        import subprocess as sp

        call_count = [0]

        def fake_run(command, **kwargs):
            call_count[0] += 1
            if "bad.gt3x" in command[1]:
                raise sp.CalledProcessError(1, command, "", "")
            r = MagicMock()
            r.stdout = r.stderr = ""
            return r

        with patch("waves_runner.subprocess.run", side_effect=fake_run):
            results = run_actinet(input_dir, output_dir, env_dir, fake_exe, FAKE_CONFIG, fake_log)

        statuses = {Path(r["file"]).name: r["status"] for r in results}
        assert statuses["good.gt3x"] == "success"
        assert statuses["bad.gt3x"] == "failed"
        assert call_count[0] == 2  # both files were attempted

    def test_no_files_returns_no_files_status(self, dirs, fake_log):
        input_dir, output_dir, env_dir = dirs
        fake_exe = env_dir / "actinet.exe"
        fake_exe.write_text("")

        results = run_actinet(input_dir, output_dir, env_dir, fake_exe, FAKE_CONFIG, fake_log)
        assert results == [{"file": "", "status": "no_files"}]


# ---------------------------------------------------------------------------
# Accelerometer command construction
# ---------------------------------------------------------------------------

class TestAccelerometerCommands:
    def test_command_uses_only_file_and_output(self, dirs, fake_log):
        """accProcess confirmed to need only: accProcess.exe file.gt3x -o output_dir"""
        input_dir, output_dir, env_dir = dirs
        (input_dir / "p001.gt3x").write_text("")
        fake_exe = env_dir / "accProcess.exe"
        fake_exe.write_text("")

        captured = []

        def fake_run(command, **kwargs):
            captured.append(command)
            r = MagicMock()
            r.stdout = r.stderr = ""
            return r

        with patch("waves_runner.subprocess.run", side_effect=fake_run):
            run_accelerometer(input_dir, output_dir, env_dir, fake_exe, FAKE_CONFIG, fake_log)

        cmd = captured[0]
        assert str(fake_exe) in cmd
        assert "-o" in cmd
        # Confirm we are NOT passing model flags (default walmsley used automatically)
        assert "--activityModel" not in cmd
        assert "--fileExtensions" not in cmd

    def test_returns_list_of_results(self, dirs, fake_log):
        input_dir, output_dir, env_dir = dirs
        (input_dir / "p001.gt3x").write_text("")
        (input_dir / "p002.gt3x").write_text("")
        fake_exe = env_dir / "accProcess.exe"
        fake_exe.write_text("")

        def fake_run(command, **kwargs):
            r = MagicMock()
            r.stdout = r.stderr = ""
            return r

        with patch("waves_runner.subprocess.run", side_effect=fake_run):
            results = run_accelerometer(input_dir, output_dir, env_dir, fake_exe, FAKE_CONFIG, fake_log)

        assert isinstance(results, list)
        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)

    def test_returns_failed_on_nonzero_exit(self, dirs, fake_log):
        input_dir, output_dir, env_dir = dirs
        (input_dir / "p001.gt3x").write_text("")
        fake_exe = env_dir / "accProcess.exe"
        fake_exe.write_text("")

        import subprocess as sp

        def fake_run(command, **kwargs):
            raise sp.CalledProcessError(1, command, "", "some error")

        with patch("waves_runner.subprocess.run", side_effect=fake_run):
            results = run_accelerometer(input_dir, output_dir, env_dir, fake_exe, FAKE_CONFIG, fake_log)

        assert results[0]["status"] == "failed"
        assert results[0]["return_code"] == 1
