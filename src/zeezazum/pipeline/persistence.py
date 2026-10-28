"""Persistência local de um perfil: posts, parquet, estatísticas e JSON bruto.

Escrita atômica: grava `<nome>.part` e troca com `os.replace`.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from zeezazum.models import Post
from zeezazum.paths import DataPaths

logger = logging.getLogger(__name__)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Grava `path` via arquivo `.part`, sem deixar o temporário no lugar."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.part")
    temporary.write_bytes(data)
    os.replace(temporary, path)


def persist_posts(paths: DataPaths, handle: str, posts: list[Post]) -> tuple[list[dict], int]:
    """Faz merge por `shortcode` e reescreve `posts.json` e `posts.parquet`.

    Devolve os posts realmente novos e o total depois do merge.
    """
    account = paths.account_dir(handle)
    new_dicts = _posts_to_dicts(posts)
    existing = _read_posts(account / "posts.json")
    seen = {item["shortcode"] for item in existing}
    fresh = [item for item in new_dicts if item["shortcode"] not in seen]
    merged = existing + fresh

    atomic_write_bytes(
        account / "posts.json",
        json.dumps(merged, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
    )
    try:
        if merged:
            frame = pl.DataFrame(merged, infer_schema_length=None)
        else:
            frame = pl.DataFrame(schema={"shortcode": pl.Utf8})
        raw = io.BytesIO()
        frame.write_parquet(raw)
        atomic_write_bytes(account / "posts.parquet", raw.getvalue())
    except Exception:
        logger.exception("falha ao gravar posts.parquet de @%s", handle)
    return fresh, len(merged)


def persist_raw(paths: DataPaths, handle: str, fresh_shortcodes: set[str], posts: list[Post]) -> Path | None:
    """Grava um chunk gzip só com os posts novos. Sem posts novos, não cria arquivo."""
    lines = [
        json.dumps({"shortcode": post.shortcode, "raw": post.raw}, ensure_ascii=False, default=str)
        for post in posts
        if post.shortcode in fresh_shortcodes
    ]
    if not lines:
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = paths.account_dir(handle) / "raw" / f"{stamp}.jsonl.gz"
    atomic_write_bytes(path, gzip.compress("\n".join(lines).encode("utf-8")))
    return path


def write_profile_stats(paths: DataPaths, handle: str, stats: dict | None, scraped_total: int) -> None:
    """Grava `profile.json` com as estatísticas do perfil e quantos posts estão salvos."""
    payload = {"handle": handle, "scraped_posts": scraped_total, **(stats or {})}
    atomic_write_bytes(
        paths.account_dir(handle) / "profile.json",
        json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
    )


def _posts_to_dicts(posts: list[Post]) -> list[dict]:
    collected_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            "shortcode": post.shortcode,
            "post_url": post.post_url,
            "timestamp": post.timestamp.isoformat() if post.timestamp else None,
            "caption": post.caption,
            "media": [{"kind": item.kind.value, "url": item.url, "index": item.index} for item in post.media],
            "is_pinned": post.is_pinned,
            "collected_at": collected_at,
            **post.metrics,
        }
        for post in posts
    ]


def _read_posts(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.exception("posts.json corrompido em %s, será sobrescrito", path)
        return []
    if not isinstance(loaded, list):
        logger.error("posts.json corrompido em %s, será sobrescrito", path)
        return []
    return loaded
