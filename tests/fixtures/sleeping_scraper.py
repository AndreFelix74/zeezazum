"""Dublê que dorme, injetado por ZEEZAZUM_TEST_SCRAPER no teste de SIGTERM."""
from __future__ import annotations

import asyncio


class _SleepingScraper:
    async def extract_posts(self, context, handle, *, max_posts, since_date):
        await asyncio.sleep(30)


def factory() -> _SleepingScraper:
    return _SleepingScraper()
