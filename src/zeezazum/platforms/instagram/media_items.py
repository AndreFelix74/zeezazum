"""Monta as linhas de TODO a partir dos itens de mídia coletados."""
from __future__ import annotations

from typing import Any

from zeezazum.models import Post
from zeezazum.paths import media_stem


def todo_records(
    posts: list[Post],
    *,
    handle: str,
    run_id: str,
    collection_stamp: str,
    collected_at: str,
) -> list[dict[str, Any]]:
    """Gera uma linha de TODO por item de mídia, na ordem dos posts e dos índices."""
    records: list[dict[str, Any]] = []
    collection_id = f"{handle}@{collection_stamp}"
    for post in posts:
        for item in sorted(post.media, key=lambda media: media.index):
            item_id = f"{post.shortcode}_{item.index}"
            records.append(
                {
                    "todo_id": f"{item_id}@{collection_stamp}",
                    "item_id": item_id,
                    "collection_id": collection_id,
                    "run_id": run_id,
                    "handle": handle,
                    "shortcode": post.shortcode,
                    "index": item.index,
                    "kind": item.kind.value,
                    "media_url": item.url,
                    "post_url": post.post_url,
                    "dest_stem": media_stem(handle, post.shortcode, item.index),
                    "collected_at": collected_at,
                }
            )
    return records
