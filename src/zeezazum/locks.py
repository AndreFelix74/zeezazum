"""Trava de instância única por processo, com ``fcntl.flock``."""
from __future__ import annotations

import errno
import fcntl
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class AlreadyRunning(Exception):
    """A trava já está ocupada por outra instância do processo."""

    def __init__(self, process_name: str) -> None:
        self.process_name = process_name
        super().__init__(f"outra instância de {process_name} está em execução")


@contextmanager
def single_instance(lock_path: Path, process_name: str) -> Iterator[None]:
    """Segura a trava exclusiva do processo enquanto o bloco estiver ativo.

    Se outra instância já tiver a trava, levanta ``AlreadyRunning``.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EACCES):
            raise AlreadyRunning(process_name) from exc
        raise
    try:
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


@contextmanager
def exit_if_running(lock_path: Path, process_name: str) -> Iterator[None]:
    """Como ``single_instance``, mas encerra com código 2 se a trava estiver ocupada."""
    try:
        with single_instance(lock_path, process_name):
            yield
    except AlreadyRunning as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(2) from exc
