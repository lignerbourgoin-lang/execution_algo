"""
Configuration Loader and Validator
-----------------------------------
Parses JSON configuration files and validates them BEFORE anything runs,
so that operators never have to touch Python source code and a typo never
turns into a silent misfire at T0.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict

SCHEDULING_MODES = ("immediate", "scheduled")


class ConfigError(ValueError):
    """Raised for any invalid configuration (fail-closed: nothing starts)."""


def parse_aware_datetime(value: Any, field_name: str) -> datetime:
    """
    Parses an ISO 8601 datetime that MUST carry a UTC offset ("2026-10-15T10:00:00+02:00" or "...Z").
    A naive time is rejected: "10:00" is ambiguous between the server's zone and yours.
    """
    if not isinstance(value, str):
        raise ConfigError(f"{field_name} must be an ISO 8601 string with offset, got {value!r}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ConfigError(f"{field_name} is not a valid ISO 8601 datetime: {value!r}") from error
    if parsed.tzinfo is None:
        raise ConfigError(f"{field_name} has no UTC offset (add +02:00 or Z): {value!r}")
    return parsed


def load_json_config(filepath: str) -> Dict[str, Any]:
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Configuration file not found: {filepath} (copy the matching .json.example and fill it in)"
        )
    with open(filepath, "r", encoding="utf-8") as config_file:
        try:
            return json.load(config_file)
        except json.JSONDecodeError as error:
            raise ConfigError(f"Invalid JSON in configuration file {filepath}: {error}") from error


def load_task_config(filepath: str) -> Dict[str, Any]:
    """Loads and validates a checkout task configuration file."""
    config = load_json_config(filepath)

    if "target" not in config or "base_url" not in config["target"]:
        raise ConfigError("Configuration missing mandatory 'target.base_url' field.")

    scheduling = config.get("scheduling", {})
    mode = scheduling.get("mode", "immediate")
    if mode not in SCHEDULING_MODES:
        raise ConfigError(f"scheduling.mode must be one of {SCHEDULING_MODES}, got {mode!r}")
    if mode == "scheduled":
        # [FEATURE: AWARE_TARGET_TIME] target_time_utc is an ISO datetime with offset, parsed here.
        # Raison: run.py used float(target_time_utc): a date string crashed at launch,
        #         and a raw epoch number gave no way to check the intended timezone.
        # Attention: the parsed datetime is stored under scheduling["target_datetime"].
        scheduling["target_datetime"] = parse_aware_datetime(
            scheduling.get("target_time_utc"), "scheduling.target_time_utc"
        )

    rate_limiting = config.get("rate_limiting", {})
    for rate_field in ("requests_per_second", "burst_capacity"):
        if rate_field in rate_limiting and float(rate_limiting[rate_field]) <= 0:
            raise ConfigError(f"rate_limiting.{rate_field} must be > 0")

    return config
