"""URLs que interrompem a coleta."""
from __future__ import annotations

import pytest

from zeezazum.platforms.instagram.blocking import BLOCKING_URL_MARKERS, is_blocking_url


@pytest.mark.parametrize("marker", BLOCKING_URL_MARKERS)
def test_each_marker_is_detected(marker: str) -> None:
    assert is_blocking_url(f"https://www.instagram.com{marker}") is True
    assert is_blocking_url(f"https://www.instagram.com{marker}?next=%2F") is True


def test_auth_platform_flow_is_blocking() -> None:
    assert is_blocking_url("https://www.instagram.com/auth_platform/no_challenge/?apc=x") is True


def test_profile_and_post_urls_are_not_blocking() -> None:
    assert is_blocking_url("https://www.instagram.com/fulano/") is False
    assert is_blocking_url("https://www.instagram.com/p/DfApM5ooxyv/") is False


def test_marker_outside_path_or_host_is_ignored() -> None:
    assert is_blocking_url("https://www.instagram.com/fulano/?next=/accounts/login/") is False
    assert is_blocking_url("https://www.facebook.com/challenge/") is False
    assert is_blocking_url("https://i.instagram.com/challenge/abc/") is True


def test_scheme_less_or_malformed_url_is_not_blocking() -> None:
    assert is_blocking_url("www.instagram.com/accounts/login") is False
    assert is_blocking_url("https://[::1") is False
