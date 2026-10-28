"""Baixa os itens de TODO e registra o resultado em DONE."""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from zeezazum.config import PipelineConfig
from zeezazum.ledger import append, read_all, read_from, utc_now_iso
from zeezazum.locks import AlreadyRunning, single_instance
from zeezazum.paths import DataPaths

logger = logging.getLogger(__name__)

_BLOCK_BYTES = 1024 * 1024
_FINAL_EXTENSIONS = (".jpg", ".png", ".mp4")


class DownloadHTTPError(Exception):
    """Resposta HTTP sem corpo salvo (4xx diferente de 429, ou 429/5xx esgotado)."""

    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.http_status = status


class _Retryable(Exception):
    """Falha que ainda cabe em outra tentativa: 429, 5xx, timeout ou conexão."""

    def __init__(self, http_status: int | None, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.http_status = http_status
        self.cause = cause


@dataclass(frozen=True, slots=True)
class _Stored:
    http_status: int
    file: str
    nbytes: int


def run_download(
    cfg: PipelineConfig,
    paths: DataPaths,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    max_cycles: int | None = None,
) -> int:
    """Executa o laço de download. Devolve o código de saída.

    ``transport`` substitui a rede nos testes. ``max_cycles`` limita os ciclos
    do laço; sem ele, o processo acompanha o TODO até um sinal.
    """
    try:
        with single_instance(paths.lock_file("download"), "download"):
            paths.ensure()
            return asyncio.run(
                _loop(cfg, paths, transport=transport, max_cycles=max_cycles)
            )
    except AlreadyRunning as exc:
        print(exc, file=sys.stderr)
        return 2


async def _loop(
    cfg: PipelineConfig,
    paths: DataPaths,
    *,
    transport: httpx.AsyncBaseTransport | None,
    max_cycles: int | None,
) -> int:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        offsets: dict[str, int] = {}
        cycles = 0
        while True:
            if stop.is_set():
                return 0
            progressed = await _drain_once(
                cfg, paths, offsets, transport=transport, stop=stop
            )
            if stop.is_set():
                return 0
            if max_cycles is not None:
                cycles += 1
                if cycles >= max_cycles:
                    return 0
            if not progressed and await _sleep_until(cfg.download.poll_interval_seconds, stop):
                return 0
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)


async def _drain_once(
    cfg: PipelineConfig,
    paths: DataPaths,
    offsets: dict[str, int],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    stop: asyncio.Event | None = None,
) -> bool:
    """Lê as linhas novas de TODO e processa cada handle com novidade.

    Atualiza ``offsets`` no lugar. Devolve True quando algum arquivo teve linha
    completa nova. Uma cauda incompleta não conta: o tamanho segue maior que o
    deslocamento, mas não há o que processar, e o laço deve dormir.
    """
    todo_dir = paths.ledger_todo
    if not todo_dir.is_dir():
        return False
    progressed = False
    for path in sorted(todo_dir.glob("*.jsonl")):
        if stop is not None and stop.is_set():
            return progressed
        handle = path.stem
        offset = offsets.get(handle, 0)
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size <= offset:
            continue
        records, new_offset = read_from(path, offset)
        if new_offset == offset:
            continue
        offsets[handle] = new_offset
        progressed = True
        if records:
            await _process_handle(
                cfg, paths, handle, records, transport=transport, stop=stop
            )
        if stop is not None and stop.is_set():
            return True
    return progressed


async def _process_handle(
    cfg: PipelineConfig,
    paths: DataPaths,
    handle: str,
    records: list[dict[str, Any]],
    *,
    transport: httpx.AsyncBaseTransport | None,
    stop: asyncio.Event | None = None,
) -> None:
    answered, ok_items = _done_sets(paths.done_file(handle))
    pending = [
        record
        for record in records
        if not _already_done(record, answered, ok_items)
        and not _kind_disabled(str(record.get("kind", "")), cfg)
    ]
    started = time.monotonic()
    if not pending:
        _log_handle(handle, [], started)
        return

    timeout = httpx.Timeout(cfg.download.request_timeout_seconds)
    client_kwargs: dict[str, Any] = {
        "timeout": timeout,
        "follow_redirects": True,
    }
    if transport is not None:
        client_kwargs["transport"] = transport
    semaphore = asyncio.Semaphore(cfg.download.max_concurrent)
    done_path = paths.done_file(handle)

    async with httpx.AsyncClient(**client_kwargs) as client:

        async def _one(record: dict[str, Any]) -> dict[str, Any] | None:
            async with semaphore:
                if stop is not None and stop.is_set():
                    return None
                result = await _download_item(
                    client,
                    record,
                    paths.output,
                    cfg.download.max_attempts,
                )
                append(done_path, result)
                return result

        results = list(await asyncio.gather(*(_one(record) for record in pending)))
    _log_handle(handle, results, started)


def _done_sets(path: Path) -> tuple[set[str], set[str]]:
    answered: set[str] = set()
    ok_items: set[str] = set()
    for row in read_all(path):
        todo_id = row.get("todo_id")
        if isinstance(todo_id, str):
            answered.add(todo_id)
        if row.get("status") == "ok":
            item_id = row.get("item_id")
            if isinstance(item_id, str):
                ok_items.add(item_id)
    return answered, ok_items


def _already_done(
    record: dict[str, Any],
    answered: set[str],
    ok_items: set[str],
) -> bool:
    todo_id = record.get("todo_id")
    if isinstance(todo_id, str) and todo_id in answered:
        return True
    item_id = record.get("item_id")
    return isinstance(item_id, str) and item_id in ok_items


def _kind_disabled(kind: str, cfg: PipelineConfig) -> bool:
    if kind == "image":
        return not cfg.download.download_images
    if kind == "video":
        return not cfg.download.download_videos
    return False


async def _download_item(
    client: httpx.AsyncClient,
    record: dict[str, Any],
    output: Path,
    max_attempts: int,
) -> dict[str, Any]:
    dest_stem = str(record["dest_stem"])
    existing = _existing_final(output, dest_stem)
    if existing is not None:
        return _done(
            record,
            status="ok",
            http_status=None,
            error=None,
            file=existing.relative_to(output).as_posix(),
            nbytes=existing.stat().st_size,
            attempts=0,
        )

    attempts = 0
    stored: _Stored | None = None
    retrying = AsyncRetrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(_Retryable),
        reraise=True,
        sleep=asyncio.sleep,
    )
    try:
        async for attempt in retrying:
            with attempt:
                attempts = attempt.retry_state.attempt_number
                stored = await _fetch_and_store(client, record, output)
    except DownloadHTTPError as exc:
        _discard_parts(output, dest_stem)
        return _done(
            record,
            status="failed",
            http_status=exc.http_status,
            error=_short_error(exc),
            file=None,
            nbytes=None,
            attempts=attempts,
        )
    except _Retryable as exc:
        _discard_parts(output, dest_stem)
        return _done(
            record,
            status="failed",
            http_status=exc.http_status,
            error=_short_error(exc.cause),
            file=None,
            nbytes=None,
            attempts=attempts,
        )
    except Exception as exc:
        if isinstance(exc, OSError):
            raise
        logger.exception(
            "falha ao baixar @%s item %s",
            record.get("handle"),
            record.get("item_id"),
        )
        _discard_parts(output, dest_stem)
        return _done(
            record,
            status="failed",
            http_status=None,
            error=_short_error(exc),
            file=None,
            nbytes=None,
            attempts=attempts,
        )
    if stored is None:
        raise RuntimeError("download terminou sem resultado")
    return _done(
        record,
        status="ok",
        http_status=stored.http_status,
        error=None,
        file=stored.file,
        nbytes=stored.nbytes,
        attempts=attempts,
    )


async def _fetch_and_store(
    client: httpx.AsyncClient,
    record: dict[str, Any],
    output: Path,
) -> _Stored:
    dest_stem = str(record["dest_stem"])
    part: Path | None = None
    renamed = False
    try:
        async with client.stream("GET", str(record["media_url"])) as response:
            status = response.status_code
            if status == 429 or status >= 500:
                raise _Retryable(status, DownloadHTTPError(status))
            if status >= 400:
                raise DownloadHTTPError(status)
            extension = _extension_for(
                str(record.get("kind", "")),
                response.headers.get("content-type"),
            )
            final = output / f"{dest_stem}{extension}"
            part = Path(f"{final}.part")
            part.parent.mkdir(parents=True, exist_ok=True)
            _unlink(part)
            written = 0
            async for chunk in response.aiter_bytes(chunk_size=_BLOCK_BYTES):
                written += await _write_blocks(part, chunk)
            if not part.exists():
                await asyncio.to_thread(_append_block, part, b"")
            await asyncio.to_thread(os.replace, part, final)
            renamed = True
            return _Stored(
                http_status=status,
                file=final.relative_to(output).as_posix(),
                nbytes=written,
            )
    except httpx.TransportError as exc:
        raise _Retryable(None, exc) from exc
    finally:
        if part is not None and not renamed:
            _unlink(part)


def _extension_for(kind: str, content_type: str | None) -> str:
    if kind == "video":
        return ".mp4"
    if content_type and "png" in content_type:
        return ".png"
    return ".jpg"


def _existing_final(output: Path, dest_stem: str) -> Path | None:
    for extension in _FINAL_EXTENSIONS:
        candidate = output / f"{dest_stem}{extension}"
        if candidate.is_file():
            return candidate
    return None


def _append_block(path: Path, data: bytes) -> None:
    with path.open("ab") as handle:
        handle.write(data)


async def _write_blocks(path: Path, chunk: bytes) -> int:
    written = 0
    rest = chunk
    while rest:
        block = rest[:_BLOCK_BYTES]
        rest = rest[_BLOCK_BYTES:]
        await asyncio.to_thread(_append_block, path, block)
        written += len(block)
    return written


def _discard_parts(output: Path, dest_stem: str) -> None:
    for extension in _FINAL_EXTENSIONS:
        _unlink(Path(f"{output / dest_stem}{extension}.part"))


def _unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _done(
    record: dict[str, Any],
    *,
    status: str,
    http_status: int | None,
    error: str | None,
    file: str | None,
    nbytes: int | None,
    attempts: int,
) -> dict[str, Any]:
    return {
        "todo_id": record["todo_id"],
        "item_id": record["item_id"],
        "status": status,
        "http_status": http_status,
        "error": error,
        "file": file,
        "bytes": nbytes,
        "attempts": attempts,
        "finished_at": utc_now_iso(),
    }


def _short_error(exc: BaseException) -> str:
    message = str(exc).splitlines()[0] if str(exc) else ""
    text = f"{type(exc).__name__}: {message}".strip()
    if len(text) > 180:
        return text[:177] + "..."
    return text


def _log_handle(
    handle: str,
    results: list[dict[str, Any] | None],
    started: float,
) -> None:
    finished = [item for item in results if item is not None]
    ok = [item for item in finished if item["status"] == "ok"]
    logger.info(
        "@%s: processados=%d ok=%d falhas=%s bytes=%d duração=%.1fs",
        handle,
        len(finished),
        len(ok),
        _failure_summary(finished),
        sum(item["bytes"] or 0 for item in ok),
        time.monotonic() - started,
    )


def _failure_summary(results: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for item in results:
        if item["status"] == "ok":
            continue
        code = "null" if item["http_status"] is None else str(item["http_status"])
        counts[code] = counts.get(code, 0) + 1
    if not counts:
        return "nenhuma"
    return ",".join(f"{code}:{counts[code]}" for code in sorted(counts))


async def _sleep_until(seconds: float, stop: asyncio.Event) -> bool:
    """Dorme até o intervalo acabar ou o sinal chegar. True se o sinal chegou."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True
