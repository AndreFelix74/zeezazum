"""Filho falso do coletor. O handle decide o resultado escrito em --result-file."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--ready-file")
    args = parser.parse_args()
    path = Path(args.result_file)
    if path.exists():
        return 2
    handle = args.handle
    if handle == "mudo":
        return 1
    if handle == "codigo3":
        return 3
    if handle == "dorminhoco":
        if args.ready_file:
            Path(args.ready_file).write_text("ok", encoding="utf-8")
        time.sleep(30)
        return 0
    if handle == "lixo":
        path.write_text("{nao-json", encoding="utf-8")
        return 1
    status = "blocked" if handle == "bloqueado" else "collected"
    payload = {
        "status": status,
        "collection_id": f"{handle}@20261028T130501Z",
        "posts_in_period": 2 if status == "collected" else 0,
        "media_items": 3 if status == "collected" else 0,
        "foreign_posts_discarded": 1 if status == "collected" else 0,
        "error": "sessão bloqueada" if status == "blocked" else None,
        "max_rss_mb": 12.5,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return 3 if status == "blocked" else 0


if __name__ == "__main__":
    sys.exit(main())
