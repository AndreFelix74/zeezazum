"""Confere se um post pertence ao perfil coletado."""
from __future__ import annotations

from zeezazum.models import Post


def is_own_post(post: Post, handle: str) -> bool:
    """Diz se o post é do perfil, como autor ou coautor.

    Dono ausente ou ``None`` não conta como próprio.
    """
    wanted = handle.lower()
    owner = post.metrics.get("owner_username")
    if isinstance(owner, str) and owner.lower() == wanted:
        return True
    coauthors = post.metrics.get("coauthors") or []
    return wanted in [author.lower() for author in coauthors if isinstance(author, str)]


def split_own_posts(posts: list[Post], handle: str) -> tuple[list[Post], int]:
    """Devolve os posts do perfil, na ordem recebida, e quantos foram descartados."""
    own = [post for post in posts if is_own_post(post, handle)]
    return own, len(posts) - len(own)
