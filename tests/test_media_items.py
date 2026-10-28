"""Linhas de TODO geradas a partir dos itens de mídia."""
from __future__ import annotations

from zeezazum.models import MediaItem, MediaKind, Post
from zeezazum.platforms.instagram.media_items import todo_records


def test_carousel_generates_one_record_per_item() -> None:
    post = Post(
        shortcode="DfApM5ooxyv",
        post_url="https://www.instagram.com/p/DfApM5ooxyv/",
        timestamp=None,
        caption=None,
        media=(
            MediaItem(MediaKind.IMAGE, "https://scontent/0.jpg", 0),
            MediaItem(MediaKind.VIDEO, "https://scontent/1.mp4", 1),
            MediaItem(MediaKind.IMAGE, "https://scontent/2.jpg", 2),
        ),
    )
    records = todo_records(
        [post],
        handle="fulano",
        run_id="lote_01",
        collection_stamp="20261028T130501Z",
        collected_at="2026-10-28T13:05:01+00:00",
    )
    assert records == [
        {
            "todo_id": "DfApM5ooxyv_0@20261028T130501Z",
            "item_id": "DfApM5ooxyv_0",
            "collection_id": "fulano@20261028T130501Z",
            "run_id": "lote_01",
            "handle": "fulano",
            "shortcode": "DfApM5ooxyv",
            "index": 0,
            "kind": "image",
            "media_url": "https://scontent/0.jpg",
            "post_url": "https://www.instagram.com/p/DfApM5ooxyv/",
            "dest_stem": "instagram/fulano/media/DfApM5ooxyv_0",
            "collected_at": "2026-10-28T13:05:01+00:00",
        },
        {
            "todo_id": "DfApM5ooxyv_1@20261028T130501Z",
            "item_id": "DfApM5ooxyv_1",
            "collection_id": "fulano@20261028T130501Z",
            "run_id": "lote_01",
            "handle": "fulano",
            "shortcode": "DfApM5ooxyv",
            "index": 1,
            "kind": "video",
            "media_url": "https://scontent/1.mp4",
            "post_url": "https://www.instagram.com/p/DfApM5ooxyv/",
            "dest_stem": "instagram/fulano/media/DfApM5ooxyv_1",
            "collected_at": "2026-10-28T13:05:01+00:00",
        },
        {
            "todo_id": "DfApM5ooxyv_2@20261028T130501Z",
            "item_id": "DfApM5ooxyv_2",
            "collection_id": "fulano@20261028T130501Z",
            "run_id": "lote_01",
            "handle": "fulano",
            "shortcode": "DfApM5ooxyv",
            "index": 2,
            "kind": "image",
            "media_url": "https://scontent/2.jpg",
            "post_url": "https://www.instagram.com/p/DfApM5ooxyv/",
            "dest_stem": "instagram/fulano/media/DfApM5ooxyv_2",
            "collected_at": "2026-10-28T13:05:01+00:00",
        },
    ]
