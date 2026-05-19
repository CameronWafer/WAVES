"""
test_validation.py
Unit tests for waves_validation.py.
Run with: python -m pytest tests/
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
import tempfile
import shutil

from waves_validation import (
    check_input_folder,
    check_output_folder,
    check_executable,
    check_input_files,
    count_files,
    find_input_files,
)


@pytest.fixture
def tmp_dir():
    d = Path(tempfile.mkdtemp())
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# check_input_folder
# ---------------------------------------------------------------------------

class TestCheckInputFolder:
    def test_valid_folder(self, tmp_dir):
        assert check_input_folder(tmp_dir) is None

    def test_nonexistent_folder(self, tmp_dir):
        missing = tmp_dir / "does_not_exist"
        result = check_input_folder(missing)
        assert result is not None
        assert "does not exist" in result.lower()

    def test_path_is_a_file(self, tmp_dir):
        f = tmp_dir / "file.txt"
        f.write_text("hello")
        result = check_input_folder(f)
        assert result is not None
        assert "not a folder" in result.lower()


# ---------------------------------------------------------------------------
# check_output_folder
# ---------------------------------------------------------------------------

class TestCheckOutputFolder:
    def test_creates_nested_folder(self, tmp_dir):
        target = tmp_dir / "a" / "b" / "c"
        assert not target.exists()
        result = check_output_folder(target)
        assert result is None
        assert target.exists()

    def test_existing_folder_is_ok(self, tmp_dir):
        assert check_output_folder(tmp_dir) is None


# ---------------------------------------------------------------------------
# check_executable
# ---------------------------------------------------------------------------

class TestCheckExecutable:
    def test_existing_exe(self, tmp_dir):
        fake_exe = tmp_dir / "actinet.exe"
        fake_exe.write_text("")
        assert check_executable(fake_exe, "ActiNet") is None

    def test_missing_exe(self, tmp_dir):
        missing = tmp_dir / "actinet.exe"
        result = check_executable(missing, "ActiNet")
        assert result is not None
        assert "reinstall" in result.lower()


# ---------------------------------------------------------------------------
# find_input_files / check_input_files / count_files
# ---------------------------------------------------------------------------

class TestInputFileScanning:
    def test_finds_gt3x_files(self, tmp_dir):
        (tmp_dir / "p001.gt3x").write_text("")
        (tmp_dir / "p002.gt3x").write_text("")
        (tmp_dir / "notes.txt").write_text("")
        files = find_input_files(tmp_dir, [".gt3x"])
        assert len(files) == 2

    def test_finds_gt3x_without_leading_dot(self, tmp_dir):
        (tmp_dir / "p001.gt3x").write_text("")
        files = find_input_files(tmp_dir, ["gt3x"])
        assert len(files) == 1

    def test_empty_folder_returns_error(self, tmp_dir):
        result = check_input_files(tmp_dir, [".gt3x"], "ActiGraph .gt3x")
        assert result is not None
        assert ".gt3x" in result

    def test_folder_with_files_returns_none(self, tmp_dir):
        (tmp_dir / "p001.gt3x").write_text("")
        result = check_input_files(tmp_dir, [".gt3x"], "ActiGraph .gt3x")
        assert result is None

    def test_count_files(self, tmp_dir):
        for i in range(5):
            (tmp_dir / f"p{i:03}.gt3x").write_text("")
        (tmp_dir / "ignore.csv").write_text("")
        assert count_files(tmp_dir, [".gt3x"]) == 5
