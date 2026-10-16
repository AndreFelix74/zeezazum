"""Orquestra a raspagem: itera candidatos sequencialmente (com delay/jitter),
decide entre raspagem completa ou incremental via StateStore, e roda o download
de mídia em paralelo (fila assíncrona) enquanto o próximo perfil é raspado.
"""
from __future__ import annotations

import asyncio
import gzip
import io
import json
import logging
import random
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from playwright.async_api import BrowserContext, async_playwright

from zeezazum.config import ZeezazumConfig
from zeezazum.models import CandidateState, Post, SocialAccount
from zeezazum.platforms.base import ProfileUnavailable
from zeezazum.platforms.instagram.downloader import MediaDownloader, MediaTask
from zeezazum.platforms.instagram.scraper import InstagramScraper
from zeezazum.pipeline.state import StateStore
from zeezazum.storage import build_storage
from zeezazum.storage.base import Storage

logger = logging.getLogger(__name__)

_STATE_KEY = "_state.duckdb"  # caminho relativo do estado dentro do Storage configurado


def _open_state(config: ZeezazumConfig, storage: Storage) -> tuple[StateStore, Path | None]:
    """DuckDB é embutido e precisa de um arquivo local pra abrir — com
    `storage.backend: local` isso já É o arquivo final. Com `s3`, baixamos o
    estado existente (se houver) pra um arquivo temporário, trabalhamos nele,
    e devolvemos o caminho pra fazer upload de volta no final — assim o estado
    de resume/raspagem incremental viaja com os dados no bucket, não fica preso
    ao disco de uma instância EC2 específica."""
    if config.storage.backend != "s3":
        local_path = Path(config.storage.local.base_path) / _STATE_KEY
        return StateStore(local_path), None

    tmp_path = Path(tempfile.gettempdir()) / f"zeezazum_state_{uuid.uuid4().hex}.duckdb"
    if storage.exists(_STATE_KEY):
        tmp_path.write_bytes(storage.read_bytes(_STATE_KEY))
    return StateStore(tmp_path), tmp_path


def _sync_state_to_remote(storage: Storage, tmp_path: Path | None) -> None:
    if tmp_path is None:
        return  # backend local: StateStore já escreveu direto no destino final
    storage.write_bytes(_STATE_KEY, tmp_path.read_bytes())
    tmp_path.unlink(missing_ok=True)


def _parse_since_date(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def _posts_to_dicts(posts: list[Post], collected_at: str | None = None) -> list[dict]:
    collected_at = collected_at or datetime.now(timezone.utc).isoformat()
    return [
        {
            "shortcode": p.shortcode,
            "post_url": p.post_url,
            "timestamp": p.timestamp.isoformat() if p.timestamp else None,
            "caption": p.caption,
            "media": [{"kind": m.kind.value, "url": m.url, "index": m.index} for m in p.media],
            "is_pinned": p.is_pinned,
            # engajamento é uma fotografia do momento da coleta — por isso o carimbo por post
            "collected_at": collected_at,
            **p.metrics,
        }
        for p in posts
    ]


def _persist_raw(storage: Storage, candidate_folder: str, fresh_shortcodes: set[str], posts: list[Post]) -> None:
    """Guarda o JSON bruto de cada post novo num chunk gzip (`raw/<ts>.jsonl.gz`),
    sem inchar posts.json. Cada chamada grava um chunk novo (sem reescrever os antigos)."""
    lines = [
        json.dumps({"shortcode": p.shortcode, "raw": p.raw}, ensure_ascii=False, default=str)
        for p in posts
        if p.shortcode in fresh_shortcodes
    ]
    if not lines:
        return
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    storage.write_bytes(f"{candidate_folder}/raw/{stamp}.jsonl.gz", gzip.compress("\n".join(lines).encode("utf-8")))


def _persist_posts(storage: Storage, candidate_folder: str, new_posts: list[Post]) -> tuple[list[dict], int]:
    """Faz merge dos posts novos com o `posts.json` já existente e reescreve
    `posts.json` + `posts.parquet`. Retorna (posts_realmente_novos, total_após_merge) —
    em modo backfill, `new_posts` normalmente inclui posts já conhecidos (a raspagem
    reprocessa do topo), então o merge é o que decide o que é de fato inédito."""
    new_dicts = _posts_to_dicts(new_posts)

    existing_dicts: list[dict] = []
    json_path = f"{candidate_folder}/posts.json"
    if storage.exists(json_path):
        try:
            existing_dicts = storage.read_json(json_path)
        except Exception:
            logger.exception("posts.json corrompido em %s, será sobrescrito", candidate_folder)

    seen = {d["shortcode"] for d in existing_dicts}
    fresh = [d for d in new_dicts if d["shortcode"] not in seen]
    merged = existing_dicts + fresh

    storage.write_json(json_path, merged)

    buffer = io.BytesIO()
    pl.DataFrame(merged, infer_schema_length=None).write_parquet(buffer)
    storage.write_bytes(f"{candidate_folder}/posts.parquet", buffer.getvalue())

    _persist_raw(storage, candidate_folder, {d["shortcode"] for d in fresh}, new_posts)
    return fresh, len(merged)


def _count_persisted(storage: Storage, candidate_folder: str) -> int:
    path = f"{candidate_folder}/posts.json"
    return len(storage.read_json(path)) if storage.exists(path) else 0


def _write_profile_stats(
    storage: Storage, candidate_folder: str, account: SocialAccount, stats: dict | None, scraped_total: int
) -> None:
    """profile.json: seguidores/total de posts no momento da coleta + quantos posts foram
    raspados (base para medir cobertura da raspagem)."""
    storage.write_json(
        f"{candidate_folder}/profile.json",
        {"handle": account.handle, "row_id": account.row_id, "scraped_posts": scraped_total, **(stats or {})},
    )


def _carry_forward_state(account: SocialAccount, existing: CandidateState | None, status: str) -> CandidateState:
    """Estado sem progresso novo (erro, ou nenhum post novo encontrado)."""
    return CandidateState(
        candidate_id=account.row_id,
        handle=account.handle,
        status=status,
        newest_post_id=existing.newest_post_id if existing else None,
        newest_post_timestamp=existing.newest_post_timestamp if existing else None,
        total_posts_seen=existing.total_posts_seen if existing else 0,
        last_run_at=datetime.now(timezone.utc),
        media_pending=existing.media_pending if existing else 0,
    )


async def _scrape_one(
    *,
    account: SocialAccount,
    context: BrowserContext,
    scraper: InstagramScraper,
    storage: Storage,
    state: StateStore,
    queue: "asyncio.Queue[MediaTask | None]",
    max_posts: int | None,
    since_date: datetime | None,
    max_attempts: int,
    backoff_seconds: int,
    extra_posts: int | None = None,
) -> None:
    """`extra_posts=None` (padrão): modo incremental — para assim que reencontra o
    post mais recente já conhecido (seguro pra cron, só traz posts novos).
    `extra_posts=N`: modo backfill — ignora esse early-stop e raspa até acumular
    `total_já_conhecido + N` posts, aprofundando no histórico de contas já raspadas."""
    existing = state.get(account.row_id)
    candidate_folder = f"instagram/{account.handle}"

    if extra_posts is None:
        stop_at_post_id = existing.newest_post_id if existing else None
        effective_max_posts = max_posts
    else:
        stop_at_post_id = None
        effective_max_posts = (existing.total_posts_seen + extra_posts) if existing else max_posts

    media_enqueued = 0

    async def flush(batch: list[Post]) -> list[dict]:
        """Persiste o lote (merge idempotente por shortcode) e enfileira a mídia só dos
        posts realmente novos — usado nos checkpoints e no final do perfil."""
        nonlocal media_enqueued
        fresh_dicts, _ = _persist_posts(storage, candidate_folder, batch)
        fresh_codes = {d["shortcode"] for d in fresh_dicts}
        for post in batch:
            if post.shortcode not in fresh_codes:
                continue
            for media_item in post.media:
                await queue.put(MediaTask(candidate_folder=candidate_folder, shortcode=post.shortcode, item=media_item))
                media_enqueued += 1
        return fresh_dicts

    posts: list[Post] | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            posts = await scraper.extract_posts(
                context,
                account.handle,
                max_posts=effective_max_posts,
                since_date=since_date,
                stop_at_post_id=stop_at_post_id,
                on_checkpoint=flush,
            )
            break
        except ProfileUnavailable as exc:
            logger.error("Perfil indisponível @%s: %s", account.handle, exc)
            state.upsert(_carry_forward_state(account, existing, status="error"))
            return
        except Exception:
            logger.exception("Erro raspando @%s (tentativa %s/%s)", account.handle, attempt, max_attempts)
            if attempt >= max_attempts:
                state.upsert(_carry_forward_state(account, existing, status="error"))
                return
            await asyncio.sleep(backoff_seconds)

    if not posts:
        logger.info("Nenhum post novo para @%s", account.handle)
        state.upsert(_carry_forward_state(account, existing, status="done"))
        return

    fresh_dicts = await flush(posts)  # posts já persistidos em checkpoints não contam como novos
    total_after_merge = _count_persisted(storage, candidate_folder)
    _write_profile_stats(storage, candidate_folder, account, scraper.last_profile_stats, total_after_merge)

    if not fresh_dicts and media_enqueued == 0:
        logger.info("Nenhum post novo para @%s", account.handle)
        state.upsert(_carry_forward_state(account, existing, status="done"))
        return

    newest = posts[0]  # extract_posts já retorna do mais novo pro mais antigo
    state.upsert(CandidateState(
        candidate_id=account.row_id,
        handle=account.handle,
        status="done",
        newest_post_id=newest.shortcode,
        newest_post_timestamp=newest.timestamp,
        total_posts_seen=total_after_merge,
        last_run_at=datetime.now(timezone.utc),
        media_pending=(existing.media_pending if existing else 0) + media_enqueued,
    ))
    logger.info("@%s: %d itens de mídia enfileirados (total: %d posts persistidos)",
                account.handle, media_enqueued, total_after_merge)


async def run_scrape(
    config: ZeezazumConfig, accounts: list[SocialAccount], *, extra_posts: int | None = None
) -> None:
    storage = build_storage(config.storage)
    state, state_tmp_path = _open_state(config, storage)
    since_date = _parse_since_date(config.scraping.since_date)

    queue: "asyncio.Queue[MediaTask | None]" = asyncio.Queue()
    downloader = MediaDownloader(
        storage,
        max_concurrent=config.media.max_concurrent_downloads,
        download_images=config.media.download_images,
        download_videos=config.media.download_videos,
    )
    consumer_task = asyncio.create_task(downloader.run(queue))

    try:
        async with async_playwright() as pw:
            browser_type = getattr(pw, config.scraping.browser)
            browser = await browser_type.launch(headless=config.scraping.headless)

            session_file = Path(config.scraping.session_file)
            if not session_file.exists():
                logger.warning(
                    "Nenhum storage_state em %s — rode scripts/local_login.py e copie o "
                    "arquivo antes de raspar perfis que exigem login.",
                    session_file,
                )
            context = await browser.new_context(
                storage_state=str(session_file) if session_file.exists() else None,
                viewport={"width": 1280, "height": 900},
            )
            scraper = InstagramScraper(scroll_delay_range=(1.5, 3.5))

            for i, account in enumerate(accounts):
                if i > 0:
                    delay = random.uniform(*config.scraping.delay_between_profiles_seconds)
                    await asyncio.sleep(delay)
                await _scrape_one(
                    account=account,
                    context=context,
                    scraper=scraper,
                    storage=storage,
                    state=state,
                    queue=queue,
                    max_posts=config.scraping.max_posts,
                    since_date=since_date,
                    max_attempts=config.scraping.retry.max_attempts,
                    backoff_seconds=config.scraping.retry.backoff_seconds,
                    extra_posts=extra_posts,
                )

            await context.close()
            await browser.close()
    finally:
        await queue.put(None)
        await consumer_task
        state.close()
        _sync_state_to_remote(storage, state_tmp_path)


async def run_media_sync(config: ZeezazumConfig, accounts: list[SocialAccount]) -> dict[str, int]:
    """Reenfileira a mídia de posts já persistidos (`posts.json`) cujo arquivo não existe em
    disco — recupera downloads perdidos por queda/erro. Usa as URLs gravadas; se o CDN já
    expirou alguma, ela falha no log e o post precisa ser re-raspado."""
    storage = build_storage(config.storage)
    queue: "asyncio.Queue[MediaTask | None]" = asyncio.Queue()
    downloader = MediaDownloader(
        storage,
        max_concurrent=config.media.max_concurrent_downloads,
        download_images=config.media.download_images,
        download_videos=config.media.download_videos,
    )
    consumer_task = asyncio.create_task(downloader.run(queue))
    queued: dict[str, int] = {}
    try:
        for account in accounts:
            folder = f"instagram/{account.handle}"
            path = f"{folder}/posts.json"
            if not storage.exists(path):
                continue
            from zeezazum.models import MediaItem, MediaKind

            n = 0
            for post in storage.read_json(path):
                for m in post.get("media", []):
                    base = f"{folder}/media/{post['shortcode']}_{m['index']}"
                    if any(storage.exists(f"{base}.{ext}") for ext in ("jpg", "png", "mp4")):
                        continue
                    item = MediaItem(kind=MediaKind(m["kind"]), url=m["url"], index=m["index"])
                    await queue.put(MediaTask(candidate_folder=folder, shortcode=post["shortcode"], item=item))
                    n += 1
            queued[account.handle] = n
    finally:
        await queue.put(None)
        await consumer_task
    return queued
