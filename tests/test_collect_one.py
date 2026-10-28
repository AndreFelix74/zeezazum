"""Coleta de um perfil com scraper substituído por um dublê."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from zeezazum.config import (
    CollectConfig,
    CollectRetryConfig,
    PipelineConfig,
    PipelinePathsConfig,
    TransferConfig,
)
from zeezazum.ledger import read_all
from zeezazum.models import MediaItem, MediaKind, Post
from zeezazum.paths import DataPaths
from zeezazum.pipeline.collect_one import collect_one
from zeezazum.pipeline.persistence import persist_posts
from zeezazum.platforms.base import ProfileUnavailable, SessionBlocked
from zeezazum.platforms.instagram.scraper import InstagramScraper, ScrapeResult, take_own_posts


def _config(data_dir: Path, *, attempts: int = 2, backoff: int = 0) -> PipelineConfig:
    return PipelineConfig(
        paths=PipelinePathsConfig(data_dir=str(data_dir)),
        collect=CollectConfig(retry=CollectRetryConfig(max_attempts=attempts, backoff_seconds=backoff)),
        transfer=TransferConfig(bucket="meu-bucket", prefix="campanha/"),
    )


def _post(shortcode: str, owner: str = "fulano", *, indexes: tuple[int, ...] = (0,)) -> Post:
    return Post(
        shortcode=shortcode,
        post_url=f"https://www.instagram.com/p/{shortcode}/",
        timestamp=None,
        caption=None,
        media=tuple(
            MediaItem(MediaKind.IMAGE if index % 2 == 0 else MediaKind.VIDEO, f"https://cdn/{shortcode}/{index}", index)
            for index in indexes
        ),
        metrics={"owner_username": owner},
        raw={"shortcode": shortcode},
    )


class _Scripted:
    def __init__(self, result: ScrapeResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls = 0

    async def extract_posts(self, context, handle: str, *, max_posts, since_date):
        self.calls += 1
        if self.error is not None and self.calls == 1 and self.result is not None:
            raise self.error
        if self.error is not None and self.result is None:
            raise self.error
        assert self.result is not None
        return self.result


def _paths(tmp_path: Path) -> DataPaths:
    return DataPaths(tmp_path / "data")


async def test_success_writes_output_before_todo_and_lists_every_post(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.ensure()
    known = _post("aaa", indexes=(0,))
    persist_posts(paths, "fulano", [known])
    fresh = _post("bbb", indexes=(0, 1))
    scripted = _Scripted(ScrapeResult(posts=[known, fresh], foreign_discarded=1, profile_stats={"followers": 10}))

    result = await collect_one(
        _config(paths.data_dir),
        paths,
        "fulano",
        "lote_01",
        scraper_factory=lambda: scripted,
    )

    assert result.status == "collected"
    assert result.posts_in_period == 2
    assert result.media_items == 3
    assert result.foreign_posts_discarded == 1
    saved = json.loads((paths.account_dir("fulano") / "posts.json").read_text(encoding="utf-8"))
    assert [item["shortcode"] for item in saved] == ["aaa", "bbb"]
    todo = read_all(paths.todo_file("fulano"))
    assert [item["item_id"] for item in todo] == ["aaa_0", "bbb_0", "bbb_1"]
    assert all(item["run_id"] == "lote_01" for item in todo)
    assert (paths.account_dir("fulano") / "profile.json").is_file()


async def test_todo_failure_keeps_output_and_writes_no_todo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path)
    scripted = _Scripted(ScrapeResult(posts=[_post("aaa")], foreign_discarded=0, profile_stats=None))

    def boom(path: Path, records: list) -> None:
        raise RuntimeError("disco cheio")

    monkeypatch.setattr("zeezazum.pipeline.collect_one.append_many", boom)
    result = await collect_one(
        _config(paths.data_dir),
        paths,
        "fulano",
        "lote_01",
        scraper_factory=lambda: scripted,
    )
    assert result.status == "error"
    assert (paths.account_dir("fulano") / "posts.json").is_file()
    assert not paths.todo_file("fulano").exists()


async def test_unavailable_and_blocked_write_no_todo(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    unavailable = _Scripted(error=ProfileUnavailable("sumiu"))
    result = await collect_one(
        _config(paths.data_dir),
        paths,
        "fulano",
        "lote_01",
        scraper_factory=lambda: unavailable,
    )
    assert result.status == "unavailable"
    assert result.error == "sumiu"
    assert not paths.todo_file("fulano").exists()

    blocked = _Scripted(error=SessionBlocked("https://www.instagram.com/accounts/login/"))
    result = await collect_one(
        _config(paths.data_dir),
        paths,
        "fulano",
        "lote_01",
        scraper_factory=lambda: blocked,
    )
    assert result.status == "blocked"
    assert blocked.calls == 1
    assert not paths.todo_file("fulano").exists()


async def test_all_discarded_posts_stay_collected_and_warn(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    paths = _paths(tmp_path)
    captured = [_post("x1", "outro"), _post("x2", "outro")]

    class _AllForeign:
        async def extract_posts(self, context, handle: str, *, max_posts, since_date):
            stats = {"followers": 10, "posts_total": 2}
            own, discarded = take_own_posts(captured, handle, stats)
            return ScrapeResult(posts=own, foreign_discarded=discarded, profile_stats=stats)

    with caplog.at_level(logging.WARNING, logger="zeezazum.platforms.instagram.scraper"):
        result = await collect_one(
            _config(paths.data_dir),
            paths,
            "fulano",
            "lote_01",
            scraper_factory=_AllForeign,
        )
    assert result.status == "collected"
    assert result.posts_in_period == 0
    assert result.media_items == 0
    assert result.foreign_posts_discarded == 2
    assert "possível mudança no formato da resposta: nenhum post com owner_username correspondente" in caplog.text
    assert "@fulano" in caplog.text
    assert "2 descartados" in caplog.text
    saved = json.loads((paths.account_dir("fulano") / "posts.json").read_text(encoding="utf-8"))
    assert saved == []


async def test_generic_error_is_retried(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    scripted = _Scripted(
        result=ScrapeResult(posts=[_post("aaa")], foreign_discarded=0, profile_stats=None),
        error=RuntimeError("rede"),
    )
    result = await collect_one(
        _config(paths.data_dir),
        paths,
        "fulano",
        "lote_01",
        scraper_factory=lambda: scripted,
    )
    assert scripted.calls == 2
    assert result.status == "collected"


class _TimeoutPage:
    def __init__(self) -> None:
        self.goto_calls = 0

    def on(self, *_args, **_kwargs) -> None:
        return None

    async def goto(self, *_args, **_kwargs) -> None:
        self.goto_calls += 1
        raise TimeoutError("tempo esgotado")

    async def close(self) -> None:
        return None


class _TimeoutContext:
    def __init__(self, page: _TimeoutPage) -> None:
        self._page = page

    async def new_page(self) -> _TimeoutPage:
        return self._page


class _GotoTimeout(InstagramScraper):
    def __init__(self, context: _TimeoutContext) -> None:
        super().__init__()
        self._context = context

    async def extract_posts(self, context, handle, *, max_posts, since_date):
        return await super().extract_posts(self._context, handle, max_posts=max_posts, since_date=since_date)


async def test_goto_timeout_is_retried_as_generic_error(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    page = _TimeoutPage()
    scraper = _GotoTimeout(_TimeoutContext(page))
    result = await collect_one(
        _config(paths.data_dir, attempts=3),
        paths,
        "fulano",
        "lote_01",
        scraper_factory=lambda: scraper,
    )
    assert page.goto_calls == 3
    assert result.status == "error"


async def test_persist_oserror_is_error_without_todo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path)
    scripted = _Scripted(ScrapeResult(posts=[_post("aaa")], foreign_discarded=0, profile_stats=None))

    def boom(*_args, **_kwargs) -> None:
        raise OSError("disco cheio")

    monkeypatch.setattr("zeezazum.pipeline.collect_one.persist_posts", boom)
    result = await collect_one(
        _config(paths.data_dir),
        paths,
        "fulano",
        "lote_01",
        scraper_factory=lambda: scripted,
    )
    assert result.status == "error"
    assert result.error == "disco cheio"
    assert not paths.todo_file("fulano").exists()
