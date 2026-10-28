"""collect-one em subprocesso: SIGTERM e falha da fábrica de teste."""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_sigterm_exits_without_result_or_todo(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config = tmp_path / "config.yaml"
    config.write_text(
        "platform: instagram\n"
        f"paths:\n  data_dir: {data_dir}\n"
        "transfer:\n  bucket: meu-bucket\n  prefix: campanha/\n",
        encoding="utf-8",
    )
    result_file = tmp_path / "result.json"
    env = os.environ.copy()
    env["ZEEZAZUM_TEST_SCRAPER"] = "tests.fixtures.sleeping_scraper:factory"
    env["PYTHONPATH"] = os.pathsep.join([str(ROOT), env.get("PYTHONPATH", "")])
    started = time.monotonic()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "zeezazum.cli",
            "collect-one",
            "--config",
            str(config),
            "--handle",
            "fulano",
            "--run-id",
            "lote_01",
            "--result-file",
            str(result_file),
        ],
        cwd=ROOT,
        env=env,
        start_new_session=True,
    )
    try:
        time.sleep(1)
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=4)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    assert time.monotonic() - started < 5
    assert code == 1
    assert not result_file.exists()
    assert not (data_dir / "ledger" / "todo" / "fulano.jsonl").exists()


def test_factory_error_writes_error_result(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config = tmp_path / "config.yaml"
    config.write_text(
        "platform: instagram\n"
        f"paths:\n  data_dir: {data_dir}\n"
        "transfer:\n  bucket: meu-bucket\n  prefix: campanha/\n",
        encoding="utf-8",
    )
    result_file = tmp_path / "result.json"
    env = os.environ.copy()
    env["ZEEZAZUM_TEST_SCRAPER"] = "tests.fixtures.raising_scraper:factory"
    env["PYTHONPATH"] = os.pathsep.join([str(ROOT), env.get("PYTHONPATH", "")])
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "zeezazum.cli",
            "collect-one",
            "--config",
            str(config),
            "--handle",
            "fulano",
            "--run-id",
            "lote_01",
            "--result-file",
            str(result_file),
        ],
        cwd=ROOT,
        env=env,
        check=False,
    )
    assert proc.returncode == 1
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload["error"] == "fábrica indisponível"
    assert re.fullmatch(r"fulano@\d{8}T\d{6}Z", payload["collection_id"])
    assert payload["posts_in_period"] == 0
    assert payload["media_items"] == 0
    assert payload["foreign_posts_discarded"] == 0
