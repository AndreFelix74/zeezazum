"""Orquestrador de coleta com filho falso."""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from zeezazum.cli import app
from zeezazum.config import (
    CollectConfig,
    PipelineConfig,
    PipelinePathsConfig,
    TransferConfig,
)
from zeezazum.ledger import append, read_all
from zeezazum.locks import single_instance
from zeezazum.paths import DataPaths
from zeezazum.pipeline.collector import run_collect

FIXTURE = Path(__file__).parent / "fixtures" / "fake_collect_child.py"
ROOT = Path(__file__).resolve().parents[1]


def _config(
    data_dir: Path,
    *,
    timeout: int = 30,
    delay: tuple[float, float] = (0, 0),
) -> PipelineConfig:
    return PipelineConfig(
        paths=PipelinePathsConfig(data_dir=str(data_dir)),
        collect=CollectConfig(
            since_date="2026-08-15",
            delay_between_profiles_seconds=delay,
            profile_timeout_seconds=timeout,
        ),
        transfer=TransferConfig(bucket="meu-bucket", prefix="campanha/"),
    )


def _paths(tmp_path: Path) -> DataPaths:
    return DataPaths(tmp_path / "data")


def _write_handles(path: Path, handles: list) -> None:
    pl.DataFrame({"handle": handles, "nota": list(range(len(handles)))}).write_parquet(path)


def _cmd(handle: str, run_id: str, result_file: Path, *, ready: Path | None = None) -> list[str]:
    command = [
        sys.executable,
        str(FIXTURE),
        "--handle",
        handle,
        "--run-id",
        run_id,
        "--result-file",
        str(result_file),
    ]
    if ready is not None:
        command.extend(["--ready-file", str(ready)])
    return command


def _run(
    tmp_path: Path,
    handles: list,
    *,
    timeout: int = 30,
    delay: tuple[float, float] = (0, 0),
    run_id: str = "lote",
    factory=None,
    ready: Path | None = None,
):
    paths = _paths(tmp_path)
    parquet = tmp_path / "entrada.parquet"
    _write_handles(parquet, handles)
    started: list[str] = []
    seen_results: list[Path] = []

    def _factory(handle: str, run: str, result_file: Path) -> list[str]:
        started.append(handle)
        seen_results.append(result_file)
        if factory is not None:
            return factory(handle, run, result_file)
        use_ready = ready if handle == "dorminhoco" else None
        return _cmd(handle, run, result_file, ready=use_ready)

    code = run_collect(
        _config(paths.data_dir, timeout=timeout, delay=delay),
        paths,
        parquet,
        run_id,
        child_cmd_factory=_factory,
    )
    return code, paths, started, seen_results, read_all(paths.collected_file(run_id))


def test_resume_skips_collected_and_unavailable(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.ensure()
    for handle, status in (("a", "collected"), ("b", "unavailable"), ("c", "error")):
        append(paths.collected_file("lote"), {"run_id": "lote", "handle": handle, "status": status})
    code, paths, started, _seen, rows = _run(tmp_path, ["a", "b", "c", "d"])
    assert code == 0
    assert started == ["c", "d"]
    fresh = [row for row in rows if row.get("collection_id")]
    assert [row["handle"] for row in fresh] == ["c", "d"]
    assert fresh[0]["status"] == "collected"
    assert fresh[0]["since_date"] == "2026-08-15"
    assert fresh[0]["posts_in_period"] == 2
    assert fresh[0]["media_items"] == 3
    assert fresh[0]["foreign_posts_discarded"] == 1
    assert fresh[0]["exit_code"] == 0
    assert fresh[0]["max_rss_mb"] == 12.5
    assert fresh[0]["started_at"].endswith("+00:00")
    assert fresh[0]["finished_at"].endswith("+00:00")


def test_blocked_stops_the_loop_and_writes_flag(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="zeezazum.pipeline.collector"):
        code, paths, started, _seen, rows = _run(tmp_path, ["a", "b", "bloqueado", "depois"])
    assert code == 3
    assert started == ["a", "b", "bloqueado"]
    assert [row["status"] for row in rows] == ["collected", "collected", "blocked"]
    flag = json.loads(paths.blocked_flag.read_text(encoding="utf-8"))
    assert flag["handle"] == "bloqueado"
    assert flag["error"] == "sessão bloqueada"
    assert flag["at"].endswith("+00:00")
    assert "resumo: collected=2 unavailable=0 error=0 blocked=1 inválidos=0" in caplog.text


def test_existing_blocked_runs_no_child(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    paths = _paths(tmp_path)
    paths.blocked_flag.parent.mkdir(parents=True)
    paths.blocked_flag.write_text("antiga", encoding="utf-8")
    with caplog.at_level(logging.CRITICAL, logger="zeezazum.pipeline.collector"):
        code, _paths_out, started, _seen, rows = _run(tmp_path, ["a", "b"])
    assert code == 3
    assert started == []
    assert rows == []
    assert paths.blocked_flag.read_text(encoding="utf-8") == "antiga"
    assert "sessão bloqueada; coleta não iniciada" in caplog.text


def test_timeout_records_error_and_continues(tmp_path: Path) -> None:
    code, _paths_out, started, _seen, rows = _run(
        tmp_path, ["dorminhoco", "depois"], timeout=1
    )
    assert code == 0
    assert started == ["dorminhoco", "depois"]
    assert rows[0]["handle"] == "dorminhoco"
    assert rows[0]["status"] == "error"
    assert rows[0]["error"] == "timeout"
    assert rows[0]["max_rss_mb"] is None
    assert rows[1]["status"] == "collected"
    assert rows[1]["max_rss_mb"] == 12.5


def test_missing_result_is_error_with_exit_code(tmp_path: Path) -> None:
    code, _paths_out, started, _seen, rows = _run(tmp_path, ["mudo", "depois"])
    assert code == 0
    assert started == ["mudo", "depois"]
    assert rows[0]["status"] == "error"
    assert rows[0]["error"] == "resultado ausente (código 1)"
    assert rows[0]["max_rss_mb"] is None
    assert rows[1]["status"] == "collected"


def test_exit_code_3_without_result_is_blocked(tmp_path: Path) -> None:
    code, paths, started, _seen, rows = _run(tmp_path, ["codigo3", "depois"])
    assert code == 3
    assert started == ["codigo3"]
    assert rows[0]["status"] == "blocked"
    assert rows[0]["exit_code"] == 3
    assert rows[0]["max_rss_mb"] is None
    flag = json.loads(paths.blocked_flag.read_text(encoding="utf-8"))
    assert flag["handle"] == "codigo3"
    assert "código 3" in flag["error"]


def test_invalid_result_is_error_with_exit_code(tmp_path: Path) -> None:
    _code, _paths_out, _started, _seen, rows = _run(tmp_path, ["lixo"])
    assert rows[0]["status"] == "error"
    assert rows[0]["error"] == "resultado inválido (código 1)"


def test_zero_delay_does_not_pause(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_args, **_kwargs):
        raise AssertionError("pausa inesperada")

    monkeypatch.setattr("zeezazum.pipeline.collector._pause", _boom)
    code, _paths_out, started, _seen, _rows = _run(tmp_path, ["a", "b"], delay=(0, 0))
    assert code == 0
    assert started == ["a", "b"]


def test_pause_happens_only_between_profiles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple] = []

    def _pause(seconds: float, _state) -> bool:
        events.append(("pause", seconds))
        return False

    def _factory(handle: str, run_id: str, result_file: Path) -> list[str]:
        events.append(("child", handle))
        return _cmd(handle, run_id, result_file)

    monkeypatch.setattr("zeezazum.pipeline.collector._pause", _pause)
    code, _paths_out, _started, _seen, _rows = _run(
        tmp_path, ["a", "b"], delay=(4, 4), factory=_factory
    )
    assert code == 0
    assert events == [("child", "a"), ("pause", 4.0), ("child", "b")]


def test_normalizes_case_and_duplicates(tmp_path: Path) -> None:
    _code, _paths_out, started, _seen, rows = _run(tmp_path, ["Fulano", "fulano", "OUTRO", ""])
    assert started == ["fulano", "outro"]
    assert [row["handle"] for row in rows] == ["fulano", "outro"]


def test_missing_handle_column_is_a_clear_error(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    parquet = tmp_path / "entrada.parquet"
    pl.DataFrame({"nome": ["fulano"]}).write_parquet(parquet)
    with pytest.raises(ValueError, match="coluna 'handle' ausente"):
        run_collect(
            _config(paths.data_dir),
            paths,
            parquet,
            "lote",
            child_cmd_factory=_cmd,
        )


def test_invalid_handles_are_warned_and_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    handles = ["Ana", "ANA", "", None, "a/b", "A/B", ".oculto", "a" * 31, "a..b", "bom_perfil"]
    with caplog.at_level(logging.INFO, logger="zeezazum.pipeline.collector"):
        code, _paths_out, started, _seen, rows = _run(tmp_path, handles)
    assert code == 0
    assert started == ["ana", "bom_perfil"]
    assert [row["handle"] for row in rows] == ["ana", "bom_perfil"]
    warned = [record.getMessage() for record in caplog.records if record.levelname == "WARNING"]
    assert warned == [
        "handle inválido descartado: a/b",
        "handle inválido descartado: .oculto",
        f"handle inválido descartado: {'a' * 31}",
        "handle inválido descartado: a..b",
    ]
    assert "inválidos=4" in caplog.text


def test_stale_result_file_is_removed_before_and_after(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    stale = paths.child_result_file("lote", "fulano")
    stale.parent.mkdir(parents=True)
    stale.write_text('{"status": "blocked", "error": "sobra"}', encoding="utf-8")
    code, paths, started, seen, rows = _run(tmp_path, ["fulano"])
    assert code == 0
    assert started == ["fulano"]
    assert seen == [paths.data_dir / "locks" / "results" / "lote__fulano.json"]
    assert rows[0]["status"] == "collected"
    assert not stale.exists()


def test_sigterm_records_interrupt_and_stops(tmp_path: Path) -> None:
    ready = tmp_path / "ready"

    def _signal_when_ready() -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not ready.exists():
            time.sleep(0.05)
        if ready.exists():
            os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=_signal_when_ready, daemon=True).start()
    code, _paths_out, started, _seen, rows = _run(
        tmp_path, ["dorminhoco", "depois"], timeout=20, ready=ready
    )
    assert code == 0
    assert started == ["dorminhoco"]
    assert rows[0]["status"] == "error"
    assert rows[0]["error"] == "interrompido"
    assert len(rows) == 1


def test_busy_lock_returns_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    paths = _paths(tmp_path)
    with single_instance(paths.lock_file("collector"), "collector"):
        code = run_collect(
            _config(paths.data_dir),
            paths,
            tmp_path / "ausente.parquet",
            "lote",
            child_cmd_factory=_cmd,
        )
    assert code == 2
    assert "outra instância de collector está em execução" in capsys.readouterr().err


def test_collector_does_not_import_playwright() -> None:
    source = (ROOT / "src" / "zeezazum" / "pipeline" / "collector.py").read_text(encoding="utf-8")
    assert "playwright" not in source


def test_help_lists_prepare_login_and_collect() -> None:
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    for name in ("prepare", "login", "collect"):
        assert name in result.stdout
    assert "collect-one" not in result.stdout


def test_run_id_defaults_to_input_stem(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str] = {}

    def _fake(cfg, paths, input_path, run_id, *, config_path=None, child_cmd_factory=None):
        seen["run_id"] = run_id
        seen["config"] = str(config_path)
        return 0

    monkeypatch.setattr("zeezazum.cli.run_collect", _fake)
    data_dir = tmp_path / "data"
    config = tmp_path / "config.yaml"
    config.write_text(
        "platform: instagram\n"
        f"paths:\n  data_dir: {data_dir}\n"
        "transfer:\n  bucket: meu-bucket\n  prefix: campanha/\n",
        encoding="utf-8",
    )
    parquet = tmp_path / "piloto.parquet"
    pl.DataFrame({"handle": ["fulano"]}).write_parquet(parquet)
    result = CliRunner().invoke(app, ["collect", "--config", str(config), "--input", str(parquet)])
    assert result.exit_code == 0
    assert seen["run_id"] == "piloto"
    assert seen["config"] == str(config)
    seen.clear()
    result = CliRunner().invoke(
        app,
        ["collect", "--config", str(config), "--input", str(parquet), "--run-id", "custom"],
    )
    assert result.exit_code == 0
    assert seen["run_id"] == "custom"


def test_unreadable_input_exits_1(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config = tmp_path / "config.yaml"
    config.write_text(
        "platform: instagram\n"
        f"paths:\n  data_dir: {data_dir}\n"
        "transfer:\n  bucket: meu-bucket\n  prefix: campanha/\n",
        encoding="utf-8",
    )
    missing = tmp_path / "ausente.parquet"
    garbage = tmp_path / "lixo.parquet"
    garbage.write_bytes(b"nao e parquet")
    runner = CliRunner()
    for path in (missing, garbage):
        result = runner.invoke(app, ["collect", "--config", str(config), "--input", str(path)])
        assert result.exit_code == 1, result.output
        assert result.stderr is not None
        assert f"não foi possível ler {path}" in result.stderr
