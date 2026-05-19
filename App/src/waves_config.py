"""
waves_config.py
Resolves application paths and loads config.json.
All path resolution for frozen (PyInstaller) and dev environments lives here.
"""

import json
import sys
from pathlib import Path


def get_app_dir() -> Path:
    """Return the directory containing the executable (or this file in dev mode)."""
    if getattr(sys, "frozen", False):
        # Running as a PyInstaller onedir build — exe is in the app root
        return Path(sys.executable).parent
    # Running from source — app root is one level up from src/
    return Path(__file__).parent.parent


def load_config() -> dict:
    config_path = get_app_dir() / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json not found at {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_actinet_exe(config: dict, app_dir: Path) -> Path:
    return app_dir / config["actinet"]["executable"]


def get_accelerometer_exe(config: dict, app_dir: Path) -> Path:
    return app_dir / config["accelerometer"]["executable"]


def get_actinet_env_dir(config: dict, app_dir: Path) -> Path:
    return app_dir / "envs" / config["actinet"]["env_name"]


def get_accelerometer_env_dir(config: dict, app_dir: Path) -> Path:
    return app_dir / "envs" / config["accelerometer"]["env_name"]


def get_default_output_dir(config: dict) -> Path:
    return Path.home() / "Documents" / config["outputs"]["default_folder_name"]


def get_log_dir(config: dict, app_dir: Path) -> Path:
    return app_dir / config["logging"]["log_folder"]
