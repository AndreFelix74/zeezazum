"""Decisão de sessão do login, sem navegador."""
from __future__ import annotations

from zeezazum.platforms.instagram.login import session_confirmed

HOME = "https://www.instagram.com/"
LOGIN = "https://www.instagram.com/accounts/login/"


def test_session_confirmed_needs_cookie_and_open_url() -> None:
    cookies = [{"name": "sessionid", "value": "abc"}]
    assert session_confirmed(cookies, HOME) is True
    assert session_confirmed([], HOME) is False
    assert session_confirmed([{"name": "sessionid", "value": ""}], HOME) is False
    assert session_confirmed([{"name": "csrftoken", "value": "abc"}], HOME) is False
    assert session_confirmed(cookies, LOGIN) is False
    assert session_confirmed([{"name": "sessionid", "value": "abc"}, {"name": "outro", "value": ""}], HOME) is True
