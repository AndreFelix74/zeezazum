"""CLI do Zeezazum."""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import resource
import signal
from pathlib import Path

import polars as pl
import typer

from zeezazum.config import load_pipeline_config
from zeezazum.input.loader import read_table, write_table
from zeezazum.input.url_parser import extract_instagram_accounts
from zeezazum.ledger import utc_stamp
from zeezazum.logsetup import configure
from zeezazum.models import SocialAccount
from zeezazum.paths import DataPaths
from zeezazum.pipeline.collect_one import CollectOneResult, collect_one
from zeezazum.pipeline.persistence import atomic_write_bytes

app = typer.Typer(help="Zeezazum — raspador universalizável de redes sociais (Playwright)")
logger = logging.getLogger(__name__)

_EXIT_CODES = {"collected": 0, "unavailable": 0, "blocked": 3, "error": 1}


def _accounts_from_table(df: pl.DataFrame, *, id_column: str, url_column: str) -> list[SocialAccount]:
    accounts: list[SocialAccount] = []
    for row_id, text in zip(df[id_column].cast(pl.Utf8), df[url_column]):
        accounts.extend(extract_instagram_accounts(str(row_id), text))
    return accounts


@app.command()
def prepare(
    input: Path = typer.Option(..., "--input", help="Tabela de origem (parquet/csv/xlsx)"),
    column: str = typer.Option(..., "--column", help="Coluna de texto livre com as redes sociais"),
    id_column: str = typer.Option(..., "--id-column", help="Coluna identificadora única de cada linha"),
    platform: str = typer.Option("instagram", "--platform", help="Plataforma a extrair (só 'instagram' por ora)"),
    extra_columns: str = typer.Option(
        "",
        "--extra-columns",
        help="Colunas adicionais da tabela de origem a manter no output, separadas por vírgula "
        "(ex.: 'NM_URNA_CANDIDATO,SG_UF,SG_PARTIDO,DS_CARGO')",
    ),
    output: Path = typer.Option(..., "--output", help="Onde salvar a tabela normalizada"),
) -> None:
    """Extrai, de uma coluna de texto livre, só as contas de uma plataforma (ex.: Instagram),
    deduplicadas por linha — gera uma tabela pequena e limpa para alimentar a coleta.
    Opcionalmente carrega junto outras colunas de identificação da tabela de origem
    (nome, UF, partido etc.), úteis pra conferir/filtrar antes de coletar."""
    if platform != "instagram":
        raise typer.BadParameter("Por enquanto só 'instagram' é suportado.")

    df = read_table(input)
    extra_cols = [c.strip() for c in extra_columns.split(",") if c.strip()]
    missing = [c for c in (column, id_column, *extra_cols) if c not in df.columns]
    if missing:
        raise typer.BadParameter(f"Coluna(s) não encontrada(s) em {input}: {missing}")

    accounts = _accounts_from_table(df, id_column=id_column, url_column=column)
    rows = [{"row_id": a.row_id, "handle": a.handle, "instagram_url": a.url} for a in accounts]
    schema = {"row_id": pl.Utf8, "handle": pl.Utf8, "instagram_url": pl.Utf8}
    result = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)

    if extra_cols:
        lookup = df.select([id_column, *extra_cols]).with_columns(pl.col(id_column).cast(pl.Utf8))
        result = result.join(lookup, left_on="row_id", right_on=id_column, how="left")
        result = result.select(["row_id", *extra_cols, "handle", "instagram_url"])

    write_table(result, output)
    typer.echo(f"{result.height} contas de instagram extraídas de {df.height} linhas -> {output}")


@app.command("collect-one", hidden=True)
def collect_one_command(
    config: Path = typer.Option(..., "--config", help="Caminho do config.yaml"),
    handle: str = typer.Option(..., "--handle", help="Perfil a coletar"),
    run_id: str = typer.Option(..., "--run-id", help="Identificador da execução"),
    result_file: Path = typer.Option(..., "--result-file", help="Onde gravar o JSON do resultado"),
) -> None:
    """Coleta um único perfil. Comando interno, oculto da ajuda."""
    cfg = load_pipeline_config(config)
    paths = DataPaths(cfg.paths.data_dir)
    configure(paths, "collector", cfg.logging.level)
    scraper_factory = None
    spec = os.environ.get("ZEEZAZUM_TEST_SCRAPER")
    if spec:
        module_name, factory_name = spec.rsplit(":", 1)
        scraper_factory = getattr(importlib.import_module(module_name), factory_name)
    exit_code = 1
    try:
        result = asyncio.run(_run_collect_one(cfg, paths, handle, run_id, scraper_factory))
        atomic_write_bytes(
            result_file,
            json.dumps(result.to_dict(), ensure_ascii=False).encode("utf-8"),
        )
        exit_code = _EXIT_CODES[result.status]
    except SystemExit as exc:
        exit_code = exc.code if isinstance(exc.code, int) else 1
    except Exception as exc:
        logger.exception("Falha ao coletar @%s", handle)
        failed = CollectOneResult(
            status="error",
            collection_id=f"{handle}@{utc_stamp()}",
            posts_in_period=0,
            media_items=0,
            foreign_posts_discarded=0,
            error=str(exc),
        )
        atomic_write_bytes(
            result_file,
            json.dumps(failed.to_dict(), ensure_ascii=False).encode("utf-8"),
        )
        exit_code = 1
    finally:
        _log_max_rss()
    raise typer.Exit(code=exit_code)


async def _run_collect_one(cfg, paths: DataPaths, handle: str, run_id: str, scraper_factory=None):
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()

    def _on_sigterm() -> None:
        task.cancel()

    loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
    try:
        return await collect_one(cfg, paths, handle, run_id, scraper_factory=scraper_factory)
    except asyncio.CancelledError:
        logger.info("coleta de @%s interrompida por SIGTERM", handle)
        raise SystemExit(1) from None
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def _log_max_rss() -> None:
    # No Linux, ru_maxrss vem em kilobytes.
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    logger.info("rss máximo: %.1f MB", rss_mb)


if __name__ == "__main__":
    app()
