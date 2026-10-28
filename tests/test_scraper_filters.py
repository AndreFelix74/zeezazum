"""Filtros de parada e de autoria do scraper, sem navegador."""
from __future__ import annotations

from datetime import datetime, timezone

from zeezazum.models import Post
from zeezazum.platforms.instagram.ownership import split_own_posts
from zeezazum.platforms.instagram.scraper import (
    _CONSECUTIVE_OLD_TO_STOP,
    _MAX_STAGNANT_SCROLLS,
    InstagramScraper,
    update_stagnation,
)

SINCE = datetime(2026, 8, 15, tzinfo=timezone.utc)


def _post(
    shortcode: str,
    when: datetime,
    owner: str,
    *,
    pinned: bool = False,
) -> Post:
    return Post(
        shortcode=shortcode,
        post_url=f"https://www.instagram.com/p/{shortcode}/",
        timestamp=when,
        caption=None,
        media=(),
        metrics={"owner_username": owner},
        is_pinned=pinned,
    )


def _own_map(posts: list[Post], handle: str) -> dict[str, Post]:
    own, _discarded = split_own_posts(posts, handle)
    return {post.shortcode: post for post in own}


def test_should_stop_ignores_foreign_posts_and_respects_max_posts() -> None:
    own = [_post("novo", datetime(2026, 9, 1, tzinfo=timezone.utc), "fulano")]
    foreign = [
        _post(f"f{i}", datetime(2026, 10, 1, tzinfo=timezone.utc), "outro") for i in range(30)
    ]
    captured = _own_map(own + foreign, "fulano")
    assert len(captured) == 1
    assert InstagramScraper._should_stop(captured, max_posts=2, since_date=SINCE) is False
    assert InstagramScraper._should_stop(captured, max_posts=1, since_date=SINCE) is True


def test_should_stop_respects_since_date_and_skips_pinned() -> None:
    old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    unpinned = [_post(f"o{i}", old, "fulano") for i in range(_CONSECUTIVE_OLD_TO_STOP - 1)]
    pinned = [_post("fixo", old, "fulano", pinned=True)]
    assert (
        InstagramScraper._should_stop(_own_map(unpinned + pinned, "fulano"), None, SINCE) is False
    )
    unpinned.append(_post("ultimo", old, "fulano"))
    assert InstagramScraper._should_stop(_own_map(unpinned, "fulano"), None, SINCE) is True


def test_apply_final_filters_respects_since_date_and_max_posts() -> None:
    posts = [
        _post("novo", datetime(2026, 10, 1, tzinfo=timezone.utc), "fulano"),
        _post("meio", datetime(2026, 9, 1, tzinfo=timezone.utc), "fulano"),
        _post("velho", datetime(2026, 1, 1, tzinfo=timezone.utc), "fulano"),
        _post("alheio", datetime(2026, 10, 2, tzinfo=timezone.utc), "outro"),
    ]
    own, discarded = split_own_posts(posts, "fulano")
    assert discarded == 1
    ordered = sorted(own, key=lambda post: post.timestamp or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    filtered = InstagramScraper._apply_final_filters(ordered, max_posts=10, since_date=SINCE)
    assert [post.shortcode for post in filtered] == ["novo", "meio"]
    capped = InstagramScraper._apply_final_filters(ordered, max_posts=1, since_date=SINCE)
    assert [post.shortcode for post in capped] == ["novo"]


def test_foreign_posts_do_not_prevent_stagnation() -> None:
    previous_own = 0
    stagnant = 0
    stopped = False
    foreign = 0
    for _ in range(_MAX_STAGNANT_SCROLLS):
        foreign += 2
        stagnant, stopped = update_stagnation(previous_own, 0, stagnant)
        previous_own = 0
    assert foreign == 2 * _MAX_STAGNANT_SCROLLS
    assert stopped is True
    stagnant, stopped = update_stagnation(previous_own, previous_own + 1, stagnant)
    assert stagnant == 0
    assert stopped is False
