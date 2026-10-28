"""Coleta um único perfil e grava `output/` e as linhas de TODO."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from playwright.async_api import async_playwright

from zeezazum.config import PipelineConfig
from zeezazum.ledger import append_many, utc_now_iso, utc_stamp
from zeezazum.models import Post
from zeezazum.paths import DataPaths
from zeezazum.pipeline.persistence import persist_posts, persist_raw, write_profile_stats
from zeezazum.platforms.base import ProfileUnavailable, SessionBlocked
from zeezazum.platforms.instagram.media_items import todo_records
from zeezazum.platforms.instagram.scraper import InstagramScraper, ScrapeResult

logger = logging.getLogger(__name__)

ScraperFactory = Callable[[], Any]
Status = Literal["collected", "unavailable", "blocked", "error"]


@dataclass
class CollectOneResult:
    status: Status
    collection_id: str
    posts_in_period: int
    media_items: int
    foreign_posts_discarded: int
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def collect_one(
    cfg: PipelineConfig,
    paths: DataPaths,
    handle: str,
    run_id: str,
    *,
    scraper_factory: ScraperFactory | None = None,
) -> CollectOneResult:
    """Coleta `handle`. Não grava COLLECTED: isso fica com o orquestrador.

    `scraper_factory`, quando informada, substitui o scraper e não abre o navegador
    (uso dos testes). Sem ela, abre o Chromium persistente em `paths.browser`.
    """
    paths.ensure()
    collection_id = f"{handle}@{utc_stamp()}"
    collected_at = utc_now_iso()
    since_date = _parse_since_date(cfg.collect.since_date)

    async def execute(context: object) -> CollectOneResult:
        scraper = (scraper_factory or InstagramScraper)()
        try:
            scraped = await _extract_with_retry(
                scraper,
                context,
                handle,
                max_posts=cfg.collect.max_posts,
                since_date=since_date,
                max_attempts=cfg.collect.retry.max_attempts,
                backoff_seconds=cfg.collect.retry.backoff_seconds,
            )
        except ProfileUnavailable as exc:
            logger.error("Perfil indisponível @%s: %s", handle, exc)
            return _empty(collection_id, "unavailable", str(exc))
        except SessionBlocked as exc:
            logger.error("Sessão bloqueada @%s: %s", handle, exc)
            return _empty(collection_id, "blocked", str(exc))
        except Exception as exc:
            return _empty(collection_id, "error", str(exc))

        try:
            fresh, total = persist_posts(paths, handle, scraped.posts)
            write_profile_stats(paths, handle, scraped.profile_stats, total)
            persist_raw(paths, handle, {item["shortcode"] for item in fresh}, scraped.posts)
        except Exception as exc:
            logger.exception("Falha ao gravar output de @%s", handle)
            return CollectOneResult(
                status="error",
                collection_id=collection_id,
                posts_in_period=len(scraped.posts),
                media_items=_count_media(scraped.posts),
                foreign_posts_discarded=scraped.foreign_discarded,
                error=str(exc),
            )
        # Cede o controle para um SIGTERM pendente sair antes de gravar TODO.
        await asyncio.sleep(0)
        try:
            _append_todo(paths, handle, run_id, collection_id, collected_at, scraped.posts)
        except Exception as exc:
            logger.exception("Falha ao gravar TODO de @%s", handle)
            return CollectOneResult(
                status="error",
                collection_id=collection_id,
                posts_in_period=len(scraped.posts),
                media_items=_count_media(scraped.posts),
                foreign_posts_discarded=scraped.foreign_discarded,
                error=str(exc),
            )
        return CollectOneResult(
            status="collected",
            collection_id=collection_id,
            posts_in_period=len(scraped.posts),
            media_items=_count_media(scraped.posts),
            foreign_posts_discarded=scraped.foreign_discarded,
            error=None,
        )

    if scraper_factory is None:
        async with async_playwright() as playwright:
            context = await playwright.chromium.launch_persistent_context(
                user_data_dir=str(paths.browser),
                headless=cfg.collect.headless,
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            try:
                return await execute(context)
            finally:
                await context.close()
    return await execute(None)


async def _extract_with_retry(
    scraper: Any,
    context: object,
    handle: str,
    *,
    max_posts: int | None,
    since_date: datetime | None,
    max_attempts: int,
    backoff_seconds: int,
) -> ScrapeResult:
    for attempt in range(1, max_attempts + 1):
        try:
            return await scraper.extract_posts(
                context,
                handle,
                max_posts=max_posts,
                since_date=since_date,
            )
        except (SessionBlocked, ProfileUnavailable):
            raise
        except Exception:
            logger.exception("Erro coletando @%s (tentativa %s/%s)", handle, attempt, max_attempts)
            if attempt >= max_attempts:
                raise
            await asyncio.sleep(backoff_seconds)
    raise AssertionError(f"tentativas esgotadas para @{handle}")


def _append_todo(
    paths: DataPaths,
    handle: str,
    run_id: str,
    collection_id: str,
    collected_at: str,
    posts: list[Post],
) -> None:
    stamp = collection_id.split("@", 1)[1]
    append_many(
        paths.todo_file(handle),
        todo_records(
            posts,
            handle=handle,
            run_id=run_id,
            collection_stamp=stamp,
            collected_at=collected_at,
        ),
    )


def _count_media(posts: list[Post]) -> int:
    return sum(len(post.media) for post in posts)


def _empty(collection_id: str, status: Status, error: str) -> CollectOneResult:
    return CollectOneResult(
        status=status,
        collection_id=collection_id,
        posts_in_period=0,
        media_items=0,
        foreign_posts_discarded=0,
        error=error,
    )


def _parse_since_date(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
