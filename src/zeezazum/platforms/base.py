"""Interface comum para scrapers de plataforma (Instagram é a primeira implementação)."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime


class ProfileUnavailable(Exception):
    """Perfil inexistente, removido ou privado."""


class SessionBlocked(Exception):
    """Login exigido, verificação de segurança ou conta suspensa; a coleta inteira deve parar."""


class BaseScraper(ABC):
    @abstractmethod
    async def extract_posts(
        self,
        context: object,
        handle: str,
        *,
        max_posts: int | None,
        since_date: datetime | None,
    ) -> object:
        """Navega até o perfil e devolve os posts próprios, do mais novo ao mais antigo.

        A raspagem para em `max_posts` ou em `since_date` (posts mais antigos que isso
        são ignorados). Posts de outros autores não entram na contagem.
        """
