"""Envia mídia e metadados ao destino e registra o resultado em UPLOADED."""
from __future__ import annotations

import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any

from zeezazum.config import PipelineConfig
from zeezazum.ledger import append, read_all, read_from, utc_now_iso
from zeezazum.locks import AlreadyRunning, single_instance
from zeezazum.paths import DataPaths, _check_component
from zeezazum.remote import ObjectStore

logger = logging.getLogger(__name__)

_BACKOFF_START = 30
_BACKOFF_MAX = 15 * 60
_META_NAMES = ("posts.json", "posts.parquet", "profile.json")


class _RemoteFailure(Exception):
    """Falha ao falar com o destino durante um envio."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.cause = cause


class _Stop:
    def __init__(self) -> None:
        self.requested = False

    def set(self) -> None:
        self.requested = True

    def is_set(self) -> bool:
        return self.requested


def run_transfer(
    cfg: PipelineConfig,
    paths: DataPaths,
    *,
    store: ObjectStore | None = None,
    max_cycles: int | None = None,
) -> int:
    """Executa o laço de transferência. Devolve o código de saída.

    ``store`` substitui o S3 nos testes. ``max_cycles`` limita os ciclos;
    sem ele, o processo acompanha o DONE até um sinal.
    """
    try:
        with single_instance(paths.lock_file("transfer"), "transfer"):
            paths.ensure()
            if store is None:
                from zeezazum.remote import S3ObjectStore

                store = S3ObjectStore(cfg.transfer.bucket, cfg.transfer.region)
            return _loop(cfg, paths, store, max_cycles=max_cycles)
    except AlreadyRunning as exc:
        print(exc, file=sys.stderr)
        return 2


def _loop(
    cfg: PipelineConfig,
    paths: DataPaths,
    store: ObjectStore,
    *,
    max_cycles: int | None,
) -> int:
    stop = _Stop()

    def _on_signal(signum: int, frame: object) -> None:
        stop.set()

    previous = {
        sig: signal.signal(sig, _on_signal) for sig in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        offsets: dict[str, int] = {}
        cache: dict[str, dict[str, tuple[int, float]]] = {}
        backup_state: dict[str, tuple[int, float]] = {}
        last_backup: float | None = None
        backoff = 0
        cycles = 0
        while True:
            if stop.is_set():
                return _shutdown(cfg, paths, store, cache, backup_state)
            progressed, failed = _drain_once(cfg, paths, offsets, store, stop)
            if stop.is_set():
                return _shutdown(cfg, paths, store, cache, backup_state)
            if not failed and _backup_due(last_backup, cfg):
                try:
                    swept = _sweep_metadata(cfg, paths, store, cache, stop)
                    backed = (
                        _backup_ledger(cfg, paths, store, backup_state, stop) if swept else False
                    )
                except _RemoteFailure as exc:
                    logger.error("falha no destino ao transferir metadados: %s", exc)
                    failed = True
                else:
                    if swept and backed:
                        last_backup = time.monotonic()
            if stop.is_set():
                return _shutdown(cfg, paths, store, cache, backup_state)
            if failed:
                backoff = _next_backoff(backoff)
                if _interruptible_sleep(backoff, stop):
                    return _shutdown(cfg, paths, store, cache, backup_state)
                continue
            backoff = 0
            if max_cycles is not None:
                cycles += 1
                if cycles >= max_cycles:
                    return 0
            if not progressed and _interruptible_sleep(cfg.transfer.poll_interval_seconds, stop):
                return _shutdown(cfg, paths, store, cache, backup_state)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _drain_once(
    cfg: PipelineConfig,
    paths: DataPaths,
    offsets: dict[str, int],
    store: ObjectStore,
    stop: _Stop | None = None,
) -> tuple[bool, bool]:
    """Processa linhas novas de DONE.

    Devolve ``(houve linha completa nova, houve falha do destino)``.
    Uma falha do destino não avança o deslocamento daquele handle.
    Uma cauda incompleta não conta como novidade.
    """
    if stop is None:
        stop = _Stop()
    done_dir = paths.ledger_done
    if not done_dir.is_dir():
        return False, False
    progressed = False
    for path in sorted(done_dir.glob("*.jsonl")):
        if stop.is_set():
            return progressed, False
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
        try:
            finished = _process_handle(cfg, paths, handle, records, store, stop)
        except _RemoteFailure as exc:
            logger.error("falha no destino ao transferir @%s: %s", handle, exc)
            return progressed, True
        if not finished:
            return progressed, False
        offsets[handle] = new_offset
        progressed = True
    return progressed, False


def _process_handle(
    cfg: PipelineConfig,
    paths: DataPaths,
    handle: str,
    records: list[dict[str, Any]],
    store: ObjectStore,
    stop: _Stop,
) -> bool:
    """Transfere a mídia do lote. Devolve False se um sinal interrompeu antes do fim."""
    media_ids = _load_media_ids(paths.uploaded_file(handle))
    transferred = 0
    errors = 0
    total_bytes = 0
    try:
        for record in records:
            if stop.is_set():
                return False
            outcome = _transfer_media(cfg, paths, handle, record, store, media_ids)
            if outcome is None:
                continue
            sent, nbytes, item_errors = outcome
            transferred += sent
            total_bytes += nbytes
            errors += item_errors
        return not stop.is_set()
    except _RemoteFailure:
        errors += 1
        raise
    finally:
        logger.info(
            "@%s: transferidos=%d bytes=%d erros=%d",
            handle,
            transferred,
            total_bytes,
            errors,
        )


def _transfer_media(
    cfg: PipelineConfig,
    paths: DataPaths,
    handle: str,
    record: dict[str, Any],
    store: ObjectStore,
    media_ids: set[str],
) -> tuple[int, int, int] | None:
    """Envia uma linha de DONE. None quando a linha não é mídia a enviar."""
    if record.get("status") != "ok":
        return None
    item_id = record.get("item_id")
    file_rel = record.get("file")
    if not isinstance(item_id, str) or not isinstance(file_rel, str):
        return None
    if item_id in media_ids or file_rel.endswith(".part"):
        return None
    local = paths.output / file_rel
    key = cfg.transfer.prefix + file_rel
    if local.is_file():
        _upload(store, local, key)
        remote_size = _head(store, key)
        local_size = local.stat().st_size
        if remote_size != local_size:
            logger.error(
                "tamanho divergente no destino para %s: local=%d destino=%s",
                key,
                local_size,
                remote_size,
            )
            return (0, 0, 1)
        if cfg.transfer.delete_local_media_after_upload:
            local.unlink()
        append(paths.uploaded_file(handle), _media_line(item_id, file_rel, key, local_size))
        media_ids.add(item_id)
        return (1, local_size, 0)

    remote_size = _head(store, key)
    expected = record.get("bytes")
    if remote_size is not None and remote_size == expected:
        append(paths.uploaded_file(handle), _media_line(item_id, file_rel, key, remote_size))
        media_ids.add(item_id)
        return (1, remote_size, 0)
    logger.error("arquivo ausente no disco e no destino: %s", file_rel)
    return (0, 0, 1)


def _sweep_metadata(
    cfg: PipelineConfig,
    paths: DataPaths,
    store: ObjectStore,
    cache: dict[str, dict[str, tuple[int, float]]],
    stop: _Stop,
) -> bool:
    """Envia metadados alterados. Devolve False se um sinal cortou a varredura.

    O estado ``meta`` de cada handle é lido uma vez e fica em ``cache``.
    ``_RemoteFailure`` interrompe a varredura.
    """
    root = paths.output / "instagram"
    if not root.is_dir():
        return not stop.is_set()
    for account in sorted(path for path in root.iterdir() if path.is_dir()):
        if stop.is_set():
            return False
        handle = account.name
        try:
            _check_component(handle, "handle")
        except ValueError:
            logger.warning("handle inválido ignorado: %s", handle)
            continue
        if handle not in cache:
            cache[handle] = _load_meta(paths.uploaded_file(handle))
        _sent, _nbytes, _errors, done = _transfer_metadata(
            cfg, paths, handle, store, cache[handle], stop
        )
        if not done:
            return False
    return not stop.is_set()


def _shutdown(
    cfg: PipelineConfig,
    paths: DataPaths,
    store: ObjectStore,
    cache: dict[str, dict[str, tuple[int, float]]],
    backup_state: dict[str, tuple[int, float]],
) -> int:
    """Última varredura e último backup, sem espera. Sai com 0 mesmo se o destino falhar."""
    quiet = _Stop()
    try:
        _sweep_metadata(cfg, paths, store, cache, quiet)
    except _RemoteFailure as exc:
        logger.error("falha na varredura final de metadados: %s", exc)
    try:
        _backup_ledger(cfg, paths, store, backup_state, quiet)
    except _RemoteFailure as exc:
        logger.error("falha no backup final dos registros: %s", exc)
    return 0


def _transfer_metadata(
    cfg: PipelineConfig,
    paths: DataPaths,
    handle: str,
    store: ObjectStore,
    meta_state: dict[str, tuple[int, float]],
    stop: _Stop,
) -> tuple[int, int, int, bool]:
    sent = 0
    total_bytes = 0
    for path in _meta_files(paths, handle):
        if stop.is_set():
            return sent, total_bytes, 0, False
        relative = path.relative_to(paths.output).as_posix()
        size = path.stat().st_size
        mtime = _mtime(path)
        previous = meta_state.get(relative)
        if previous is not None and previous == (size, mtime):
            continue
        key = cfg.transfer.prefix + relative
        _upload(store, path, key)
        append(paths.uploaded_file(handle), _meta_line(relative, key, size, mtime))
        meta_state[relative] = (size, mtime)
        sent += 1
        total_bytes += size
    return sent, total_bytes, 0, True


def _meta_files(paths: DataPaths, handle: str) -> list[Path]:
    account = paths.account_dir(handle)
    files = [account / name for name in _META_NAMES if (account / name).is_file()]
    raw = account / "raw"
    if raw.is_dir():
        files.extend(
            sorted(
                path
                for path in raw.iterdir()
                if path.is_file() and not path.name.endswith(".part")
            )
        )
    return files


def _load_media_ids(path: Path) -> set[str]:
    media_ids: set[str] = set()
    for row in read_all(path):
        if row.get("kind") != "media":
            continue
        item_id = row.get("item_id")
        if isinstance(item_id, str):
            media_ids.add(item_id)
    return media_ids


def _load_meta(path: Path) -> dict[str, tuple[int, float]]:
    meta: dict[str, tuple[int, float]] = {}
    for row in read_all(path):
        if row.get("kind") != "meta":
            continue
        file_rel = row.get("file")
        if isinstance(file_rel, str):
            meta[file_rel] = (int(row.get("bytes") or 0), float(row.get("local_mtime") or 0))
    return meta


def _upload(store: ObjectStore, local_path: Path, key: str) -> None:
    try:
        store.upload_file(local_path, key)
    except _RemoteFailure:
        raise
    except Exception as exc:
        logger.error("falha ao enviar %s: %s", key, exc)
        raise _RemoteFailure(exc) from exc


def _head(store: ObjectStore, key: str) -> int | None:
    try:
        return store.head_size(key)
    except _RemoteFailure:
        raise
    except Exception as exc:
        logger.error("falha ao consultar %s: %s", key, exc)
        raise _RemoteFailure(exc) from exc


def _media_line(item_id: str, file_rel: str, key: str, nbytes: int) -> dict[str, Any]:
    return {
        "kind": "media",
        "item_id": item_id,
        "file": file_rel,
        "key": key,
        "bytes": nbytes,
        "local_mtime": None,
        "uploaded_at": utc_now_iso(),
    }


def _meta_line(file_rel: str, key: str, nbytes: int, mtime: float) -> dict[str, Any]:
    return {
        "kind": "meta",
        "item_id": None,
        "file": file_rel,
        "key": key,
        "bytes": nbytes,
        "local_mtime": mtime,
        "uploaded_at": utc_now_iso(),
    }


def _mtime(path: Path) -> float:
    return float(f"{path.stat().st_mtime:.6f}")


def _backup_due(last_backup: float | None, cfg: PipelineConfig) -> bool:
    if last_backup is None:
        return True
    elapsed = time.monotonic() - last_backup
    return elapsed >= cfg.transfer.ledger_backup_interval_seconds


def _backup_ledger(
    cfg: PipelineConfig,
    paths: DataPaths,
    store: ObjectStore,
    state: dict[str, tuple[int, float]],
    stop: _Stop,
) -> bool:
    """Envia os registros alterados. Devolve False se um sinal cortou o backup."""
    ledger_root = paths.ledger_done.parent
    if not ledger_root.is_dir():
        return True
    for path in sorted(ledger_root.rglob("*")):
        if stop.is_set():
            return False
        if not path.is_file() or path.name.endswith(".part"):
            continue
        relative = path.relative_to(ledger_root).as_posix()
        stat = path.stat()
        signature = (stat.st_size, stat.st_mtime)
        if state.get(relative) == signature:
            continue
        key = f"{cfg.transfer.prefix}_ledger/{relative}"
        _upload(store, path, key)
        state[relative] = signature
    return True


def _next_backoff(current: int) -> int:
    if current <= 0:
        return _BACKOFF_START
    return min(current * 2, _BACKOFF_MAX)


def _interruptible_sleep(seconds: float, stop: _Stop) -> bool:
    """Dorme. Devolve True se o sinal chegou durante a espera."""
    deadline = time.monotonic() + seconds
    while True:
        if stop.is_set():
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.2, remaining))
