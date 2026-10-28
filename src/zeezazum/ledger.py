"""Leitura e escrita dos registros JSONL do pipeline."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    """Data e hora UTC no formato ISO 8601 com fuso (``AAAA-MM-DDTHH:MM:SS+00:00``)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def utc_stamp() -> str:
    """Carimbo UTC compacto ``YYYYMMDDTHHMMSSZ``."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def append(path: Path, record: dict[str, Any]) -> None:
    """Acrescenta um registro e garante que a linha chegou ao disco.

    Se o arquivo existe, não está vazio e o último byte não é ``\\n``, grava um
    ``\\n`` antes da linha nova, na mesma abertura. Isso isola uma linha
    interrompida por queda, para que o registro seguinte não seja perdido.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
    with path.open("ab+") as handle:
        size = handle.seek(0, os.SEEK_END)
        if size > 0:
            handle.seek(size - 1)
            if handle.read(1) != b"\n":
                handle.write(b"\n")
        handle.write(line.encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())


def read_all(path: Path) -> list[dict[str, Any]]:
    """Lê todos os registros completos. Arquivo inexistente devolve lista vazia."""
    if not path.is_file():
        return []
    records, _consumed = _parse(path.read_bytes(), path, offset=0)
    return records


def read_from(path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    """Lê as linhas completas a partir de ``offset`` (bytes).

    Devolve os registros novos e o deslocamento do fim da última linha completa.
    Uma linha incompleta no fim não avança o deslocamento.
    """
    if offset < 0:
        raise ValueError("offset não pode ser negativo")
    if not path.is_file():
        return [], offset
    with path.open("rb") as handle:
        handle.seek(offset)
        data = handle.read()
    records, consumed = _parse(data, path, offset=offset)
    return records, offset + consumed


def _line_number_at(path: Path, offset: int) -> int:
    if offset <= 0:
        return 1
    with path.open("rb") as handle:
        remaining = offset
        count = 0
        while remaining:
            chunk = handle.read(min(remaining, 1024 * 1024))
            if not chunk:
                break
            count += chunk.count(b"\n")
            remaining -= len(chunk)
    return count + 1


def _parse(data: bytes, path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    """Interpreta o prefixo de linhas completas de ``data``.

    Devolve os objetos JSON e quantos bytes esse prefixo ocupa.
    A última linha do arquivo, se não terminar em ``\\n`` ou não for JSON válido,
    é ignorada sem aviso. Linhas inválidas no meio geram ``WARNING``.
    O número da linha só é calculado quando há uma dessas linhas a reportar.
    """
    if not data:
        return [], 0
    file_ends_with_newline = data.endswith(b"\n")
    if file_ends_with_newline:
        complete = data
    else:
        newline_at = data.rfind(b"\n")
        if newline_at == -1:
            return [], 0
        complete = data[: newline_at + 1]

    lines = complete.split(b"\n")[:-1]
    last_index = len(lines) - 1
    invalid_middle: list[int] = []
    records: list[dict[str, Any]] = []
    for index, raw in enumerate(lines):
        is_last_line_of_file = file_ends_with_newline and index == last_index
        parsed = _load_record(raw)
        if parsed is None:
            if not is_last_line_of_file:
                invalid_middle.append(index)
            continue
        records.append(parsed)
    if invalid_middle:
        start_line = _line_number_at(path, offset)
        for index in invalid_middle:
            logger.warning("linha inválida ignorada em %s:%d", path, start_line + index)
    return records, len(complete)


def _load_record(raw: bytes) -> dict[str, Any] | None:
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed
