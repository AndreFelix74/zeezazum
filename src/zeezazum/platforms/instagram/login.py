"""Login manual no perfil persistente do Chromium."""
from __future__ import annotations

import asyncio
import logging
import sys

from playwright.async_api import async_playwright

from zeezazum.locks import AlreadyRunning, single_instance
from zeezazum.paths import DataPaths
from zeezazum.platforms.instagram.blocking import is_blocking_url
from zeezazum.platforms.instagram.browser import launch_profile

logger = logging.getLogger(__name__)

_LOGIN_PROMPT = (
    "Faça login na janela do navegador, abra um perfil para confirmar "
    "que os posts carregam e pressione Enter aqui."
)


def session_confirmed(cookies: list[dict], url: str) -> bool:
    """Há sessão se existir cookie sessionid não vazio e a URL não for de bloqueio."""
    has_session = any(
        cookie.get("name") == "sessionid" and isinstance(cookie.get("value"), str) and cookie["value"]
        for cookie in cookies
    )
    return has_session and not is_blocking_url(url)


def run_login(paths: DataPaths) -> int:
    """Abre o navegador para o login. Devolve o código de saída."""
    try:
        with single_instance(paths.lock_file("collector"), "collector"):
            return asyncio.run(_login(paths))
    except AlreadyRunning as exc:
        print(exc, file=sys.stderr)
        return 2


async def _login(paths: DataPaths) -> int:
    paths.ensure()
    async with async_playwright() as playwright:
        context = await launch_profile(playwright, paths, headless=False)
        try:
            page = await context.new_page()
            await page.goto("https://www.instagram.com/accounts/login/")
            print(_LOGIN_PROMPT)
            await asyncio.to_thread(input)
            await page.goto("https://www.instagram.com/")
            cookies = await context.cookies("https://www.instagram.com")
            confirmed = session_confirmed(cookies, page.url)
        finally:
            await context.close()
    if not confirmed:
        logger.warning("sessão ainda bloqueada após o login; BLOCKED não foi removido")
        print("A sessão ainda está bloqueada. O arquivo BLOCKED não foi removido.", file=sys.stderr)
        return 3
    if paths.blocked_flag.exists():
        paths.blocked_flag.unlink()
    logger.info("login confirmado")
    return 0
