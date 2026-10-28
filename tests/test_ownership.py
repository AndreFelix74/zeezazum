"""Autoria dos posts em relação ao perfil coletado."""
from __future__ import annotations

from zeezazum.models import Post
from zeezazum.platforms.instagram.ownership import is_own_post, split_own_posts


def _post(**metrics: object) -> Post:
    return Post(
        shortcode="abc",
        post_url="https://www.instagram.com/p/abc/",
        timestamp=None,
        caption=None,
        media=(),
        metrics=metrics,
    )


def test_owner_match_ignores_case() -> None:
    assert is_own_post(_post(owner_username="Fulano"), "FULANO") is True


def test_coauthor_match_ignores_case() -> None:
    post = _post(owner_username="outro", coauthors=["Beltrano", "FuLaNo"])
    assert is_own_post(post, "fulano") is True


def test_other_author_is_rejected() -> None:
    post = _post(owner_username="outro", coauthors=["beltrano"])
    assert is_own_post(post, "fulano") is False


def test_missing_owner_is_rejected() -> None:
    assert is_own_post(_post(), "fulano") is False
    assert is_own_post(_post(owner_username=None), "fulano") is False


def test_none_in_coauthors_is_ignored() -> None:
    matched = _post(owner_username="outro", coauthors=[None, "Fulano"])
    assert is_own_post(matched, "fulano") is True
    only_none = _post(owner_username="outro", coauthors=[None])
    assert is_own_post(only_none, "fulano") is False
    absent = _post(owner_username="outro", coauthors=None)
    assert is_own_post(absent, "fulano") is False


def test_non_string_coauthors_are_ignored() -> None:
    post = _post(coauthors=[{"username": "fulano"}, 123, "beltrano"])
    assert is_own_post(post, "fulano") is False


def test_split_own_posts_counts_discarded() -> None:
    own_owner = _post(owner_username="Fulano")
    foreign = _post(owner_username="outro")
    own_coauthor = _post(coauthors=[None, "fulano"])
    own, discarded = split_own_posts([own_owner, foreign, own_coauthor], "fulano")
    assert own == [own_owner, own_coauthor]
    assert discarded == 1
