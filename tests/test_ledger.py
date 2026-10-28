"""Leitura e escrita dos registros JSONL."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from zeezazum.ledger import append, read_all, read_from, utc_now_iso, utc_stamp


def test_append_then_read_all_preserves_order(tmp_path: Path) -> None:
    path = tmp_path / "ledger" / "todo" / "fulano.jsonl"
    append(path, {"n": 1, "nome": "não"})
    append(path, {"n": 2})
    assert path.is_file()
    assert read_all(path) == [{"n": 1, "nome": "não"}, {"n": 2}]


def test_read_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "ausente.jsonl"
    assert read_all(path) == []
    assert read_from(path, 0) == ([], 0)


def test_incomplete_last_line_is_ignored_and_does_not_advance(tmp_path: Path) -> None:
    path = tmp_path / "todo.jsonl"
    complete = b'{"a": 1}\n'
    path.write_bytes(complete + b'{"b": 2}')
    assert read_all(path) == [{"a": 1}]
    records, offset = read_from(path, 0)
    assert records == [{"a": 1}]
    assert offset == len(complete)
    again, offset_again = read_from(path, offset)
    assert again == []
    assert offset_again == offset


def test_invalid_middle_line_warns(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    path = tmp_path / "todo.jsonl"
    path.write_bytes(b'{"a": 1}\nisto-nao-e-json\n{"b": 2}\n')
    with caplog.at_level(logging.WARNING, logger="zeezazum.ledger"):
        records = read_all(path)
    assert records == [{"a": 1}, {"b": 2}]
    assert f"{path}:2" in caplog.text


def test_invalid_last_line_is_silent(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    path = tmp_path / "todo.jsonl"
    path.write_bytes(b'{"a": 1}\nisto-nao-e-json\n')
    with caplog.at_level(logging.WARNING, logger="zeezazum.ledger"):
        records = read_all(path)
    assert records == [{"a": 1}]
    assert caplog.records == []


def test_read_from_returns_only_new_lines(tmp_path: Path) -> None:
    path = tmp_path / "done.jsonl"
    append(path, {"n": 1})
    first, offset = read_from(path, 0)
    assert first == [{"n": 1}]
    append(path, {"n": 2})
    second, offset = read_from(path, offset)
    assert second == [{"n": 2}]
    third, offset_again = read_from(path, offset)
    assert third == []
    assert offset_again == offset


def test_utc_helpers() -> None:
    iso = utc_now_iso()
    stamp = utc_stamp()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00", iso)
    assert re.fullmatch(r"\d{8}T\d{6}Z", stamp)
    parsed = datetime.fromisoformat(iso)
    assert parsed.utcoffset() == timedelta(0)
