"""Trava de instância única."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from zeezazum.locks import single_instance


def _run_lock(lock_path: Path, process_name: str) -> subprocess.CompletedProcess[str]:
    script = (
        "import sys\n"
        "from pathlib import Path\n"
        "from zeezazum.locks import AlreadyRunning, single_instance\n"
        "path, name = sys.argv[1], sys.argv[2]\n"
        "try:\n"
        "    with single_instance(Path(path), name):\n"
        "        pass\n"
        "except AlreadyRunning as exc:\n"
        "    print(exc, file=sys.stderr)\n"
        "    sys.exit(3)\n"
    )
    return subprocess.run(
        [sys.executable, "-c", script, str(lock_path), process_name],
        capture_output=True,
        text=True,
        check=False,
    )


def test_second_process_raises_and_lock_can_be_reacquired(tmp_path: Path) -> None:
    lock_path = tmp_path / "locks" / "collector.lock"
    with single_instance(lock_path, "collect"):
        blocked = _run_lock(lock_path, "collect")
        assert blocked.returncode == 3, blocked.stderr
        assert "outra instância de collect está em execução" in blocked.stderr
    with single_instance(lock_path, "collect"):
        pass
    acquired = _run_lock(lock_path, "collect")
    assert acquired.returncode == 0, acquired.stderr


def test_exit_if_running_exits_with_code_2(tmp_path: Path) -> None:
    lock_path = tmp_path / "download.lock"
    script = (
        "import sys\n"
        "from pathlib import Path\n"
        "from zeezazum.locks import exit_if_running\n"
        "path, name = sys.argv[1], sys.argv[2]\n"
        "with exit_if_running(Path(path), name):\n"
        "    print('entrou')\n"
    )
    with single_instance(lock_path, "download"):
        blocked = subprocess.run(
            [sys.executable, "-c", script, str(lock_path), "download"],
            capture_output=True,
            text=True,
            check=False,
        )
    assert blocked.returncode == 2, blocked.stderr
    assert "outra instância de download está em execução" in blocked.stderr
    assert "entrou" not in blocked.stdout
