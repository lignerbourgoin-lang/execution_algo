from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from .models import EventType, Target

ROOT = Path(__file__).resolve().parent.parent


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML invalide: {path}")
    return data


def load_family(path: Path | None = None) -> dict[str, Any]:
    return _load_yaml(path or ROOT / "family.yaml")


def load_events(path: Path | None = None) -> dict[str, Any]:
    return _load_yaml(path or ROOT / "events.yaml")


def _parse_t0(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value).timestamp()


def build_target(event_id: str, family: dict | None = None, events: dict | None = None) -> Target:
    family = family or load_family()
    events = events or load_events()
    defaults = events.get("defaults") or {}
    burst = defaults.get("burst") or {}
    found = None
    for ev in events.get("events") or []:
        if ev.get("id") == event_id:
            found = ev
            break
    if not found:
        raise KeyError(f"event inconnu: {event_id}")

    quantity = int(family.get("quantity", 1))
    if found.get("quantity") is not None:
        quantity = min(quantity, int(found["quantity"]))

    poll = found.get("poll_seconds") or defaults.get("poll_seconds") or [2.0, 4.0]
    session = found.get("session") or {}
    cap = int(found.get("account_cap") or quantity)

    return Target(
        id=found["id"],
        type=EventType(found["type"]),
        adapter=found.get("adapter", "generic_json"),
        quantity=quantity,
        account_cap=cap,
        categories=list(found.get("categories") or []),
        urls=dict(found.get("urls") or {}),
        session_headers={str(k): str(v) for k, v in (session.get("headers") or {}).items()},
        session_cookies={str(k): str(v) for k, v in (session.get("cookies") or {}).items()},
        t0=_parse_t0(found.get("t0")),
        poll_min=float(poll[0]),
        poll_max=float(poll[-1]),
        burst_shots=int(found.get("burst", burst).get("shots", burst.get("shots", 5))),
        burst_spacing=int(found.get("burst", burst).get("spacing_ms", burst.get("spacing_ms", 80))) / 1000.0,
        max_price=found.get("max_price"),
        admit_timeout_s=float(found.get("admit_timeout_s") or defaults.get("admit_timeout_s") or 7200),
    )


def list_event_ids(events: dict | None = None) -> list[str]:
    events = events or load_events()
    return [str(ev["id"]) for ev in events.get("events") or [] if "id" in ev]
