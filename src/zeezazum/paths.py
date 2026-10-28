"""Caminhos do diretório de dados do pipeline.

Único lugar que monta caminhos sob ``paths.data_dir``.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def _check_component(value: str, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith(".")
        or "/" in value
        or "\\" in value
        or ".." in value
    ):
        raise ValueError(
            f"{label} inválido: não pode ser vazio, conter '/', '\\' ou '..', nem começar com '.'"
        )
    return value


def media_stem(handle: str, shortcode: str, index: int) -> str:
    """Caminho relativo de um item de mídia, sem extensão."""
    safe_handle = _check_component(handle, "handle")
    safe_shortcode = _check_component(shortcode, "shortcode")
    return f"instagram/{safe_handle}/media/{safe_shortcode}_{index}"


@dataclass(frozen=True, slots=True)
class DataPaths:
    """Caminhos derivados de um diretório de dados."""

    data_dir: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_dir", Path(self.data_dir))

    @property
    def output(self) -> Path:
        return self.data_dir / "output"

    @property
    def ledger_todo(self) -> Path:
        return self.data_dir / "ledger" / "todo"

    @property
    def ledger_done(self) -> Path:
        return self.data_dir / "ledger" / "done"

    @property
    def ledger_uploaded(self) -> Path:
        return self.data_dir / "ledger" / "uploaded"

    @property
    def ledger_collected(self) -> Path:
        return self.data_dir / "ledger" / "collected"

    @property
    def reports(self) -> Path:
        return self.data_dir / "reports"

    @property
    def browser(self) -> Path:
        return self.data_dir / "browser"

    @property
    def locks(self) -> Path:
        return self.data_dir / "locks"

    @property
    def blocked_flag(self) -> Path:
        return self.data_dir / "BLOCKED"

    @property
    def logs(self) -> Path:
        return self.data_dir / "logs"

    def account_dir(self, handle: str) -> Path:
        return self.output / "instagram" / _check_component(handle, "handle")

    def todo_file(self, handle: str) -> Path:
        return self.ledger_todo / f"{_check_component(handle, 'handle')}.jsonl"

    def done_file(self, handle: str) -> Path:
        return self.ledger_done / f"{_check_component(handle, 'handle')}.jsonl"

    def uploaded_file(self, handle: str) -> Path:
        return self.ledger_uploaded / f"{_check_component(handle, 'handle')}.jsonl"

    def collected_file(self, run_id: str) -> Path:
        return self.ledger_collected / f"{_check_component(run_id, 'run_id')}.jsonl"

    def lock_file(self, name: str) -> Path:
        return self.locks / f"{name}.lock"

    def log_file(self, name: str) -> Path:
        return self.logs / f"{name}.log"

    def ensure(self) -> None:
        """Cria os diretórios do layout. Não cria o arquivo ``BLOCKED``."""
        for directory in (
            self.output / "instagram",
            self.ledger_todo,
            self.ledger_done,
            self.ledger_uploaded,
            self.ledger_collected,
            self.reports,
            self.browser,
            self.locks,
            self.logs,
        ):
            directory.mkdir(parents=True, exist_ok=True)
