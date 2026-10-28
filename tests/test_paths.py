"""Caminhos derivados do diretório de dados."""
from __future__ import annotations

from pathlib import Path

import pytest

from zeezazum.paths import DataPaths, media_stem


def test_derived_paths(tmp_path: Path) -> None:
    paths = DataPaths(tmp_path)
    assert paths.output == tmp_path / "output"
    assert paths.ledger_todo == tmp_path / "ledger" / "todo"
    assert paths.ledger_done == tmp_path / "ledger" / "done"
    assert paths.ledger_uploaded == tmp_path / "ledger" / "uploaded"
    assert paths.ledger_collected == tmp_path / "ledger" / "collected"
    assert paths.reports == tmp_path / "reports"
    assert paths.browser == tmp_path / "browser"
    assert paths.locks == tmp_path / "locks"
    assert paths.blocked_flag == tmp_path / "BLOCKED"
    assert paths.logs == tmp_path / "logs"
    assert paths.account_dir("fulano") == tmp_path / "output" / "instagram" / "fulano"
    assert paths.todo_file("fulano") == tmp_path / "ledger" / "todo" / "fulano.jsonl"
    assert paths.done_file("fulano") == tmp_path / "ledger" / "done" / "fulano.jsonl"
    assert paths.uploaded_file("fulano") == tmp_path / "ledger" / "uploaded" / "fulano.jsonl"
    assert paths.collected_file("lote_01") == tmp_path / "ledger" / "collected" / "lote_01.jsonl"
    assert paths.lock_file("collector") == tmp_path / "locks" / "collector.lock"
    assert paths.log_file("collector") == tmp_path / "logs" / "collector.log"
    assert paths.todo_file("fulano.silva").name == "fulano.silva.jsonl"


def test_ensure_creates_directories(tmp_path: Path) -> None:
    paths = DataPaths(tmp_path / "data")
    paths.ensure()
    for directory in (
        paths.output,
        paths.output / "instagram",
        paths.ledger_todo,
        paths.ledger_done,
        paths.ledger_uploaded,
        paths.ledger_collected,
        paths.reports,
        paths.browser,
        paths.locks,
        paths.logs,
    ):
        assert directory.is_dir()
    assert not paths.blocked_flag.exists()


@pytest.mark.parametrize(
    "handle",
    ["a/b", "a\\b", "..", ".oculto", "foo/../bar", "a..b"],
)
def test_invalid_handle(tmp_path: Path, handle: str) -> None:
    paths = DataPaths(tmp_path)
    with pytest.raises(ValueError):
        paths.account_dir(handle)
    with pytest.raises(ValueError):
        paths.todo_file(handle)
    with pytest.raises(ValueError):
        paths.done_file(handle)
    with pytest.raises(ValueError):
        paths.uploaded_file(handle)


def test_media_stem() -> None:
    assert media_stem("fulano", "DfApM5ooxyv", 0) == "instagram/fulano/media/DfApM5ooxyv_0"
    with pytest.raises(ValueError):
        media_stem("a/b", "DfApM5ooxyv", 0)


def test_empty_handle_and_run_id_raise(tmp_path: Path) -> None:
    paths = DataPaths(tmp_path)
    with pytest.raises(ValueError):
        paths.account_dir("")
    with pytest.raises(ValueError):
        paths.collected_file("")


@pytest.mark.parametrize("run_id", ["a/b", "a\\b", "..", ".oculto", "lote/../outro", "a..b"])
def test_invalid_run_id(tmp_path: Path, run_id: str) -> None:
    paths = DataPaths(tmp_path)
    with pytest.raises(ValueError):
        paths.collected_file(run_id)
