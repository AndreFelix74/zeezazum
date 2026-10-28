"""Reconhece URLs que interrompem a coleta do Instagram."""
from __future__ import annotations

from urllib.parse import urlsplit

BLOCKING_URL_MARKERS = (
    "/accounts/login",
    "/challenge/",
    "/accounts/suspended",
    "/accounts/disabled",
)


def is_blocking_url(url: str) -> bool:
    """Diz se a URL é login, verificação ou suspensão no host do Instagram.

    Exige esquema e host ``instagram.com`` ou um subdomínio. O path precisa
    começar com um marcador. URL sem esquema ou malformada devolve ``False``.
    """
    try:
        parts = urlsplit(url)
        host = parts.hostname
    except ValueError:
        return False
    if not parts.scheme or not host:
        return False
    if host != "instagram.com" and not host.endswith(".instagram.com"):
        return False
    return any(parts.path.startswith(marker) for marker in BLOCKING_URL_MARKERS)
