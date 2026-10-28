"""Extração de contas a partir de texto livre."""
from __future__ import annotations

from zeezazum.input.url_parser import extract_instagram_accounts
from zeezazum.models import SocialAccount


def test_instagram_paths_are_not_profiles() -> None:
    text = "; ".join(
        [
            "https://www.instagram.com/channel/abc",
            "https://www.instagram.com/direct/t/123",
            "https://instagram.com/s/abc",
            "instagram.com/share/p/abc",
            "https://www.instagram.com/about/",
            "https://www.instagram.com/legal/terms",
            "https://www.instagram.com/web/",
            "https://www.instagram.com/developer/",
            "https://www.instagram.com/p/DfApM5ooxyv/",
            "https://www.instagram.com/reel/DfApM5ooxyv/",
            "https://www.instagram.com/reels/DfApM5ooxyv/",
            "https://www.instagram.com/stories/fulano/",
            "https://www.instagram.com/explore/tags/x/",
            "https://www.instagram.com/tv/DfApM5ooxyv/",
            "https://www.instagram.com/accounts/login/",
        ]
    )
    assert extract_instagram_accounts("1", text) == []


def test_profile_urls_labels_and_loose_handles() -> None:
    text = (
        "https://www.Instagram.com/Fulano/; "
        "instagram.com/pr-fulano\n"
        "http://instagram.com/@ciclano; "
        "instagram.com/xx@beltrano; "
        "Link Instagram: @deltano; "
        "INSTAGRAM: @fulano; "
        "Link Instagram: eugenio"
    )
    accounts = extract_instagram_accounts("42", text)
    assert accounts == [
        SocialAccount("42", "instagram", "fulano", "https://www.instagram.com/fulano/"),
        SocialAccount("42", "instagram", "pr-fulano", "https://www.instagram.com/pr-fulano/"),
        SocialAccount("42", "instagram", "ciclano", "https://www.instagram.com/ciclano/"),
        SocialAccount("42", "instagram", "beltrano", "https://www.instagram.com/beltrano/"),
        SocialAccount("42", "instagram", "deltano", "https://www.instagram.com/deltano/"),
        SocialAccount("42", "instagram", "eugenio", "https://www.instagram.com/eugenio/"),
    ]


def test_empty_text_yields_nothing() -> None:
    assert extract_instagram_accounts("1", None) == []
    assert extract_instagram_accounts("1", "") == []
    assert extract_instagram_accounts("1", "   ") == []
