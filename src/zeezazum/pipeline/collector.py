"""Orquestra a coleta: um processo filho por perfil e uma linha em COLLECTED."""
from __future__ import annotations

import json
import logging
import os
import random
import re
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from zeezazum.config import PipelineConfig
from zeezazum.ledger import append, read_all, utc_now_iso, utc_stamp
from zeezazum.locks import AlreadyRunning, single_instance
from zeezazum.paths import DataPaths, _check_component
from zeezazum.pipeline.persistence import atomic_write_bytes

logger = logging.getLogger(__name__)

_HANDLE_RE = re.compile(r"^[a-z0-9._]{1,30}$")
_RESUME_STATUSES = {"collected", "unavailable"}
_RESULT_STATUSES = {"collected", "unavailable", "error", "blocked"}
_STOP_GRACE_SECONDS = 30

ChildCmdFactory = Callable[[str, str, Path], list[str]]


@dataclass
class _SignalState:
    interrupted: bool = False
    child_pid: int | None = None


@dataclass
class _Outcome:
    record: dict[str, Any]
    duration: float
    stop: str | None


def run_collect(
    cfg: PipelineConfig,
    paths: DataPaths,
    input_path: Path,
    run_id: str,
    *,
    config_path: Path | None = None,
    child_cmd_factory: ChildCmdFactory | None = None,
) -> int:
    """Coleta os handles do parquet. Devolve o código de saída.

    ``child_cmd_factory(handle, run_id, result_file)`` substitui o comando do
    filho nos testes. Sem ela, executa ``collect-one`` e ``config_path`` é obrigatório.
    """
    try:
        with single_instance(paths.lock_file("collector"), "collector"):
            return _collect_locked(
                cfg,
                paths,
                input_path,
                run_id,
                config_path=config_path,
                child_cmd_factory=child_cmd_factory,
            )
    except AlreadyRunning as exc:
        print(exc, file=sys.stderr)
        return 2


def _collect_locked(
    cfg: PipelineConfig,
    paths: DataPaths,
    input_path: Path,
    run_id: str,
    *,
    config_path: Path | None,
    child_cmd_factory: ChildCmdFactory | None,
) -> int:
    if paths.blocked_flag.exists():
        logger.critical("sessão bloqueada; coleta não iniciada (%s)", paths.blocked_flag)
        return 3

    paths.ensure()
    pending, invalid_count = _pending_handles(paths, input_path, run_id)
    state = _SignalState()
    previous = _install_signals(state)
    counts = {"collected": 0, "unavailable": 0, "error": 0, "blocked": 0}
    code = 0
    try:
        for index, handle in enumerate(pending):
            if state.interrupted:
                break
            if index > 0:
                delay = random.uniform(*cfg.collect.delay_between_profiles_seconds)
                if delay > 0 and _pause(delay, state):
                    break
            outcome = _run_child(
                cfg,
                paths,
                run_id,
                handle,
                state,
                config_path=config_path,
                child_cmd_factory=child_cmd_factory,
            )
            _log_profile(outcome)
            status = outcome.record["status"]
            counts[status] = counts.get(status, 0) + 1
            if outcome.stop == "blocked":
                _write_blocked(paths, handle, outcome.record.get("error"))
                logger.critical("sessão bloqueada em @%s: %s", handle, outcome.record.get("error"))
                code = 3
                break
            if outcome.stop == "interrompido":
                break
    finally:
        _restore_signals(previous)
    _log_summary(counts, invalid_count)
    return code


def _pending_handles(paths: DataPaths, input_path: Path, run_id: str) -> tuple[list[str], int]:
    frame = pl.read_parquet(input_path)
    if "handle" not in frame.columns:
        raise ValueError(f"coluna 'handle' ausente em {input_path}")
    normalized = _normalize_handles(frame.get_column("handle").to_list())
    valid: list[str] = []
    invalid = 0
    for handle in normalized:
        if _valid_handle(handle):
            valid.append(handle)
            continue
        invalid += 1
        logger.warning("handle inválido descartado: %s", handle)
    done = _resumed_handles(paths, run_id)
    return [handle for handle in valid if handle not in done], invalid


def _normalize_handles(values: list[Any]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value).strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _valid_handle(handle: str) -> bool:
    if _HANDLE_RE.fullmatch(handle) is None:
        return False
    try:
        _check_component(handle, "handle")
    except ValueError:
        return False
    return True


def _resumed_handles(paths: DataPaths, run_id: str) -> set[str]:
    done: set[str] = set()
    for record in read_all(paths.collected_file(run_id)):
        handle = record.get("handle")
        if record.get("status") in _RESUME_STATUSES and isinstance(handle, str):
            done.add(handle)
    return done


def _install_signals(state: _SignalState) -> dict[int, Any]:
    def _on_signal(signum: int, frame: object) -> None:
        state.interrupted = True
        pid = state.child_pid
        if pid is None:
            return
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            return

    previous: dict[int, Any] = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, _on_signal)
    return previous


def _restore_signals(previous: dict[int, Any]) -> None:
    for sig, handler in previous.items():
        signal.signal(sig, handler)


def _pause(seconds: float, state: _SignalState) -> bool:
    """Espera ``seconds``. Devolve True se um sinal interrompeu a espera."""
    deadline = time.monotonic() + seconds
    while not state.interrupted:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.2, remaining))
    return True


def _run_child(
    cfg: PipelineConfig,
    paths: DataPaths,
    run_id: str,
    handle: str,
    state: _SignalState,
    *,
    config_path: Path | None,
    child_cmd_factory: ChildCmdFactory | None,
) -> _Outcome:
    result_path = paths.child_result_file(run_id, handle)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.unlink(missing_ok=True)
    command = _child_command(
        handle,
        run_id,
        result_path,
        config_path=config_path,
        child_cmd_factory=child_cmd_factory,
    )
    started_at = utc_now_iso()
    started = time.monotonic()
    proc = subprocess.Popen(command, stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        state.child_pid = proc.pid
        kind, exit_code = _wait_child(proc, cfg.collect.profile_timeout_seconds, state)
    finally:
        state.child_pid = None
    duration = time.monotonic() - started
    finished_at = utc_now_iso()

    if kind == "ok":
        parsed = _consume_result(result_path, exit_code)
        status = parsed["status"]
        error = parsed["error"]
        posts = parsed["posts_in_period"]
        media = parsed["media_items"]
        discarded = parsed["foreign_posts_discarded"]
        collection_id = parsed["collection_id"] or f"{handle}@{utc_stamp()}"
        max_rss_mb = parsed["max_rss_mb"]
        stop = "blocked" if status == "blocked" else None
        if exit_code == 3:
            status = "blocked"
            stop = "blocked"
    else:
        result_path.unlink(missing_ok=True)
        status = "error"
        error = "timeout" if kind == "timeout" else "interrompido"
        posts = 0
        media = 0
        discarded = 0
        collection_id = f"{handle}@{utc_stamp()}"
        max_rss_mb = None
        stop = "interrompido" if kind == "interrompido" else None

    record = {
        "run_id": run_id,
        "handle": handle,
        "collection_id": collection_id,
        "status": status,
        "since_date": cfg.collect.since_date,
        "posts_in_period": posts,
        "media_items": media,
        "foreign_posts_discarded": discarded,
        "error": error,
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "max_rss_mb": max_rss_mb,
    }
    append(paths.collected_file(run_id), record)
    return _Outcome(record=record, duration=duration, stop=stop)


def _child_command(
    handle: str,
    run_id: str,
    result_file: Path,
    *,
    config_path: Path | None,
    child_cmd_factory: ChildCmdFactory | None,
) -> list[str]:
    if child_cmd_factory is not None:
        return child_cmd_factory(handle, run_id, result_file)
    if config_path is None:
        raise ValueError("config_path é obrigatório para executar collect-one")
    return [
        sys.executable,
        "-m",
        "zeezazum.cli",
        "collect-one",
        "--config",
        str(config_path),
        "--handle",
        handle,
        "--run-id",
        run_id,
        "--result-file",
        str(result_file),
    ]


def _wait_child(proc: subprocess.Popen, timeout: float, state: _SignalState) -> tuple[str, int]:
    deadline = time.monotonic() + timeout
    while True:
        if state.interrupted:
            return "interrompido", _stop_child(proc)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "timeout", _stop_child(proc)
        try:
            proc.wait(timeout=min(0.2, remaining))
        except subprocess.TimeoutExpired:
            continue
        if state.interrupted:
            return "interrompido", _exit_code(proc)
        return "ok", _exit_code(proc)


def _stop_child(proc: subprocess.Popen) -> int:
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=_STOP_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=_STOP_GRACE_SECONDS)
    return _exit_code(proc)


def _exit_code(proc: subprocess.Popen) -> int:
    if proc.returncode is None:
        return 1
    return proc.returncode


def _consume_result(path: Path, exit_code: int) -> dict[str, Any]:
    try:
        return _parse_result(path, exit_code)
    finally:
        path.unlink(missing_ok=True)


def _parse_result(path: Path, exit_code: int) -> dict[str, Any]:
    if not path.is_file():
        return _failed_result(exit_code, f"resultado ausente (código {exit_code})")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return _failed_result(exit_code, f"resultado inválido (código {exit_code})")
    if not isinstance(payload, dict) or payload.get("status") not in _RESULT_STATUSES:
        return _failed_result(exit_code, f"resultado inválido (código {exit_code})")
    try:
        posts = int(payload.get("posts_in_period") or 0)
        media = int(payload.get("media_items") or 0)
        discarded = int(payload.get("foreign_posts_discarded") or 0)
    except (TypeError, ValueError):
        return _failed_result(exit_code, f"resultado inválido (código {exit_code})")
    collection_id = payload.get("collection_id")
    if not isinstance(collection_id, str) or not collection_id:
        collection_id = None
    error = payload.get("error")
    if error is not None and not isinstance(error, str):
        error = str(error)
    return {
        "status": payload["status"],
        "collection_id": collection_id,
        "posts_in_period": posts,
        "media_items": media,
        "foreign_posts_discarded": discarded,
        "error": error,
        "max_rss_mb": _optional_rss(payload.get("max_rss_mb")),
    }


def _failed_result(exit_code: int, error: str) -> dict[str, Any]:
    return {
        "status": "error",
        "collection_id": None,
        "posts_in_period": 0,
        "media_items": 0,
        "foreign_posts_discarded": 0,
        "error": error,
        "max_rss_mb": None,
    }


def _optional_rss(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _write_blocked(paths: DataPaths, handle: str, error: str | None) -> None:
    payload = {"at": utc_now_iso(), "handle": handle, "error": error}
    atomic_write_bytes(paths.blocked_flag, json.dumps(payload, ensure_ascii=False).encode("utf-8"))


def _log_profile(outcome: _Outcome) -> None:
    record = outcome.record
    rss = record["max_rss_mb"]
    logger.info(
        "@%s: status=%s posts=%d mídia=%d descartados=%d duração=%.1fs rss=%s",
        record["handle"],
        record["status"],
        record["posts_in_period"],
        record["media_items"],
        record["foreign_posts_discarded"],
        outcome.duration,
        "null" if rss is None else f"{rss:.1f}MB",
    )


def _log_summary(counts: dict[str, int], invalid_count: int) -> None:
    logger.info(
        "resumo: collected=%d unavailable=%d error=%d blocked=%d inválidos=%d",
        counts.get("collected", 0),
        counts.get("unavailable", 0),
        counts.get("error", 0),
        counts.get("blocked", 0),
        invalid_count,
    )

