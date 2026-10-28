"""Configuração de log de cada processo do pipeline."""
from __future__ import annotations

import logging

from zeezazum.paths import DataPaths


def configure(paths: DataPaths, process_name: str, level: str) -> None:
    """Grava o log em ``paths.log_file(process_name)`` com o formato do projeto.

    ``httpx`` e ``botocore`` ficam em ``WARNING``.
    """
    log_path = paths.log_file(process_name)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level,
        filename=str(log_path),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
