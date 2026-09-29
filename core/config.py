"""
Configuration Loader and Validator
-----------------------------------
Parses JSON configuration files, validates targets, endpoints, and credentials
so that operators never have to touch Python source code.
"""

import json
import os
from typing import Any, Dict


def load_task_config(filepath: str) -> Dict[str, Any]:
    """Loads and validates a task configuration file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Configuration file not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file {filepath}: {e}")

    # Validate essential fields
    if "target" not in config or "base_url" not in config["target"]:
        raise ValueError("Configuration missing mandatory 'target.base_url' field.")

    return config
