"""Abertura do perfil persistente do Chromium."""
from __future__ import annotations

from playwright.async_api import BrowserContext, Playwright

from zeezazum.paths import DataPaths


async def launch_profile(playwright: Playwright, paths: DataPaths, *, headless: bool) -> BrowserContext:
    """Abre o contexto persistente usado pela coleta e pelo login."""
    return await playwright.chromium.launch_persistent_context(
        user_data_dir=str(paths.browser),
        headless=headless,
        viewport={"width": 1280, "height": 900},
        locale="en-US",
    )
