"""Merge e escrita atômica dos arquivos de um perfil."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

from zeezazum.models import Post
from zeezazum.paths import DataPaths
from zeezazum.pipeline.persistence import persist_posts, persist_raw


def _post(shortcode: str) -> Post:
    return Post(
        shortcode=shortcode,
        post_url=f"https://www.instagram.com/p/{shortcode}/",
        timestamp=None,
        caption=None,
        media=(),
        raw={"shortcode": shortcode},
    )


def test_merge_does_not_duplicate_shortcode_and_leaves_no_part(tmp_path: Path) -> None:
    paths = DataPaths(tmp_path)
    paths.ensure()
    persist_posts(paths, "fulano", [_post("aaa"), _post("bbb")])
    persist_posts(paths, "fulano", [_post("bbb"), _post("ccc")])
    saved = json.loads((paths.account_dir("fulano") / "posts.json").read_text(encoding="utf-8"))
    assert [item["shortcode"] for item in saved] == ["aaa", "bbb", "ccc"]
    assert (paths.account_dir("fulano") / "posts.parquet").is_file()
    assert list(paths.account_dir("fulano").rglob("*.part")) == []


def test_incompatible_metric_types_keep_posts_json(tmp_path: Path) -> None:
    paths = DataPaths(tmp_path)
    paths.ensure()
    posts = [
        Post(
            shortcode="aaa",
            post_url="https://www.instagram.com/p/aaa/",
            timestamp=None,
            caption=None,
            media=(),
            metrics={"like_count": 3},
        ),
        Post(
            shortcode="bbb",
            post_url="https://www.instagram.com/p/bbb/",
            timestamp=None,
            caption=None,
            media=(),
            metrics={"like_count": "muitos"},
        ),
    ]
    fresh, total = persist_posts(paths, "fulano", posts)
    saved = json.loads((paths.account_dir("fulano") / "posts.json").read_text(encoding="utf-8"))
    assert [item["shortcode"] for item in saved] == ["aaa", "bbb"]
    assert [item["like_count"] for item in saved] == [3, "muitos"]
    assert len(fresh) == 2
    assert total == 2


def test_raw_chunk_contains_only_new_posts(tmp_path: Path) -> None:
    paths = DataPaths(tmp_path)
    paths.ensure()
    first = [_post("aaa"), _post("bbb")]
    fresh, _total = persist_posts(paths, "fulano", first)
    first_chunk = persist_raw(paths, "fulano", {item["shortcode"] for item in fresh}, first)
    assert first_chunk is not None
    assert _shortcodes(first_chunk) == {"aaa", "bbb"}

    second = [_post("aaa"), _post("bbb"), _post("ccc")]
    fresh, _total = persist_posts(paths, "fulano", second)
    second_chunk = persist_raw(paths, "fulano", {item["shortcode"] for item in fresh}, second)
    assert second_chunk is not None
    assert second_chunk != first_chunk
    assert _shortcodes(second_chunk) == {"ccc"}
    assert len(list((paths.account_dir("fulano") / "raw").glob("*.jsonl.gz"))) == 2
    assert list(paths.account_dir("fulano").rglob("*.part")) == []


def _shortcodes(path: Path) -> set[str]:
    payload = gzip.decompress(path.read_bytes()).decode("utf-8")
    return {json.loads(line)["shortcode"] for line in payload.splitlines()}
