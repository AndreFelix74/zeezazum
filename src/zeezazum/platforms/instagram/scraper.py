"""Scraper do Instagram: navega até o perfil e captura os posts via interceptação
de rede (mesma filosofia do Zeeschuimer — captura o que a própria página carrega,
não faz scraping de DOM nem chama API "por fora" do browser autenticado).
"""
from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from playwright.async_api import BrowserContext, Response

from zeezazum.models import Post
from zeezazum.platforms.base import BaseScraper, ProfileUnavailable, SessionBlocked
from zeezazum.platforms.instagram.blocking import is_blocking_url
from zeezazum.platforms.instagram.ownership import split_own_posts
from zeezazum.platforms.instagram.parser import parse_posts_from_payload

logger = logging.getLogger(__name__)

_API_URL_HINTS = ("graphql", "/api/v1/")
_UNAVAILABLE_WAIT_SECONDS = 10
_MAX_STAGNANT_SCROLLS = 8  # perfis longos: o Instagram às vezes demora a devolver a próxima página
_CONSECUTIVE_OLD_TO_STOP = 12  # posts não fixados mais antigos que since_date para encerrar o scroll
_LONG_PAUSE_EVERY = 40  # scrolls entre pausas longas (reduz risco de bloqueio em scroll profundo)
_LONG_PAUSE_RANGE = (8.0, 20.0)
_OG_STATS_RE = re.compile(
    r"([\d.,]+)\s*([KkMm])?\s+Followers?,\s*([\d.,]+)\s*([KkMm])?\s+Following,\s*([\d.,]+)\s*([KkMm])?\s+Posts?",
    re.I,
)
_MIN_DT = datetime.min.replace(tzinfo=timezone.utc)


@dataclass
class ScrapeResult:
    """Posts próprios já filtrados, quantos alheios foram descartados e as estatísticas do perfil."""

    posts: list[Post]
    foreign_discarded: int
    profile_stats: dict | None


def take_own_posts(
    captured: list[Post], handle: str, profile_stats: dict | None
) -> tuple[list[Post], int]:
    """Separa os posts do perfil. Avisa se havia posts e nenhum era do autor."""
    own, discarded = split_own_posts(captured, handle)
    if profile_stats is not None and captured and not own:
        logger.warning(
            "@%s: possível mudança no formato da resposta: nenhum post com owner_username correspondente (%d descartados)",
            handle,
            discarded,
        )
    return own, discarded


def update_stagnation(previous_own: int, own_count: int, stagnant_rounds: int) -> tuple[int, bool]:
    """Compara a quantidade de posts próprios entre rodadas.

    Devolve as rodadas estagnadas e se a rolagem deve parar. Posts alheios
    não entram na conta.
    """
    if own_count == previous_own:
        stagnant_rounds += 1
    else:
        stagnant_rounds = 0
    return stagnant_rounds, stagnant_rounds >= _MAX_STAGNANT_SCROLLS


async def _wait_for_own_post(captured: dict[str, Post], handle: str) -> None:
    """Aguarda um post próprio, no máximo `_UNAVAILABLE_WAIT_SECONDS`, a cada 0,5 s."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _UNAVAILABLE_WAIT_SECONDS
    while True:
        own, _discarded = split_own_posts(list(captured.values()), handle)
        if own or loop.time() >= deadline:
            return
        await asyncio.sleep(min(0.5, deadline - loop.time()))


class InstagramScraper(BaseScraper):
    def __init__(self, *, scroll_delay_range: tuple[float, float] = (1.5, 3.5)):
        self._scroll_delay_range = scroll_delay_range

    async def extract_posts(
        self,
        context: BrowserContext,
        handle: str,
        *,
        max_posts: int | None,
        since_date: datetime | None,
    ) -> ScrapeResult:
        page = await context.new_page()
        captured: dict[str, Post] = {}
        blocking_url: str | None = None

        async def on_response(response: Response) -> None:
            nonlocal blocking_url
            if is_blocking_url(response.url):
                blocking_url = response.url
                return
            if not any(hint in response.url for hint in _API_URL_HINTS):
                return
            # sem filtro por content-type: o Instagram serve o GraphQL da timeline com
            # content-type que nem sempre contém "json" (text/javascript etc.); response.json()
            # já falha silenciosamente abaixo quando o corpo não é JSON.
            try:
                payload = await response.json()
            except Exception:
                return
            for post in parse_posts_from_payload(payload):
                captured.setdefault(post.shortcode, post)

        async def raise_if_blocked() -> None:
            page_blocked = is_blocking_url(page.url)
            if not page_blocked and blocking_url is None:
                return
            url = page.url if page_blocked else blocking_url
            raise SessionBlocked(f"Sessão bloqueada ao acessar @{handle}: {url}")

        page.on("response", on_response)
        try:
            await page.goto(f"https://www.instagram.com/{handle}/", wait_until="domcontentloaded")
            await _wait_for_own_post(captured, handle)

            await raise_if_blocked()
            profile_stats = await self._read_profile_stats(page)
            own_now, _discarded = split_own_posts(list(captured.values()), handle)
            if profile_stats is None and not own_now:
                raise ProfileUnavailable(f"Perfil indisponível: @{handle}")

            stagnant_rounds = 0
            previous_own = 0
            scrolls = 0
            while True:
                own_now, _discarded = split_own_posts(list(captured.values()), handle)
                own_map = {post.shortcode: post for post in own_now}
                if self._should_stop(own_map, max_posts, since_date):
                    break
                # o mouse precisa estar sobre o conteúdo: em (0,0) o wheel cai na barra lateral
                # fixa do Instagram e a grade não rola (bug visto no piloto: só 3 posts por perfil).
                await page.mouse.move(random.uniform(500, 800), random.uniform(350, 550))
                await page.mouse.wheel(0, 3000)
                await asyncio.sleep(random.uniform(*self._scroll_delay_range))
                scrolls += 1
                if scrolls % _LONG_PAUSE_EVERY == 0:
                    await asyncio.sleep(random.uniform(*_LONG_PAUSE_RANGE))
                await raise_if_blocked()

                own_after, _discarded = split_own_posts(list(captured.values()), handle)
                stagnant_rounds, stop = update_stagnation(previous_own, len(own_after), stagnant_rounds)
                previous_own = len(own_after)
                if stop:
                    break

            own, discarded = take_own_posts(list(captured.values()), handle, profile_stats)
            ordered = sorted(own, key=lambda post: post.timestamp or _MIN_DT, reverse=True)
            posts = self._apply_final_filters(ordered, max_posts, since_date)
            return ScrapeResult(posts=posts, foreign_discarded=discarded, profile_stats=profile_stats)
        finally:
            try:
                await page.close()
            except Exception:
                logger.debug("falha ao fechar a página de @%s", handle, exc_info=True)

    @staticmethod
    def _should_stop(
        captured: dict[str, Post],
        max_posts: int | None,
        since_date: datetime | None,
    ) -> bool:
        if max_posts is not None and len(captured) >= max_posts:
            return True
        if since_date is not None:
            # posts fixados no topo da grade podem ser bem antigos e não indicam que o
            # scroll passou do corte — só contam posts não fixados.
            old = sum(
                1
                for post in captured.values()
                if not post.is_pinned and post.timestamp and post.timestamp < since_date
            )
            if old >= _CONSECUTIVE_OLD_TO_STOP:
                return True
        return False

    @staticmethod
    async def _read_profile_stats(page) -> dict | None:
        """Lê seguidores/seguindo/total de posts da meta og:description da página do perfil."""
        try:
            content = await page.locator('meta[property="og:description"]').first.get_attribute(
                "content", timeout=5000
            )
        except Exception:
            return None
        match = _OG_STATS_RE.search(content or "")
        if not match:
            return None

        def num(raw: str, suffix: str | None) -> int:
            value = float(raw.replace(",", ""))
            return round(value * {"k": 1e3, "m": 1e6}.get((suffix or "").lower(), 1))

        return {
            "followers": num(match.group(1), match.group(2)),
            "following": num(match.group(3), match.group(4)),
            "posts_total": num(match.group(5), match.group(6)),
            "followers_rounded": bool(match.group(2)),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _apply_final_filters(
        posts: list[Post],
        max_posts: int | None,
        since_date: datetime | None,
    ) -> list[Post]:
        result: list[Post] = []
        for post in posts:
            if since_date is not None and post.timestamp and post.timestamp < since_date:
                break
            result.append(post)
            if max_posts is not None and len(result) >= max_posts:
                break
        return result
