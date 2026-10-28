"""Dublê cuja fábrica falha ao ser chamada."""
from __future__ import annotations


def factory() -> None:
    raise RuntimeError("fábrica indisponível")
