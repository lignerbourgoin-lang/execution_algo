from __future__ import annotations

import argparse
import asyncio

from .config import build_target, list_event_ids, load_family
from .engine import Engine
from .notify import make_notify


def main() -> None:
    p = argparse.ArgumentParser(prog="sniper")
    p.add_argument("--event", help="id dans events.yaml")
    p.add_argument("--list", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.list:
        print("\n".join(list_event_ids()) or "(aucun event)")
        return
    if not args.event:
        p.error("--event est requis (ou --list)")

    family = load_family()
    target = build_target(args.event, family=family)
    engine = Engine(target, make_notify(family))
    asyncio.run(engine.run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
