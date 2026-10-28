# Etapa 03: coleta de um perfil (`collect-one`)

Pré-requisitos: etapas 01 e 02 concluídas e revisadas; leitura de `00-arquitetura.md`.

## Objetivo

Reescrever o scraper e criar o comando interno que coleta **um único perfil** e produz `output/` e as linhas de TODO. Nesta etapa o fluxo antigo é removido, porque o scraper muda de interface.

## Escopo

### 1. Scraper (`platforms/instagram/scraper.py`)

Nova assinatura:

```python
async def extract_posts(self, context: BrowserContext, handle: str, *,
                        max_posts: int | None, since_date: datetime | None) -> ScrapeResult
```

`ScrapeResult` (dataclass): `posts: list[Post]` (só posts próprios, do mais novo ao mais antigo, já filtrados por `since_date` e `max_posts`), `foreign_discarded: int`, `profile_stats: dict | None`.

Alterações:

1. **Remover** `stop_at_post_id`, `on_checkpoint`, `_CHECKPOINT_EVERY` e o atributo `last_profile_stats` (as estatísticas passam a vir no `ScrapeResult`). Manter `_should_stop` e `_apply_final_filters` sem o parâmetro removido.
2. **Bloqueio de sessão**: no `on_response`, se `is_blocking_url(response.url)`, marcar a detecção. Após `page.goto` e a cada iteração de rolagem, verificar a marcação e `is_blocking_url(page.url)`; se positivo, fechar a página e levantar `SessionBlocked` com o handle e a URL.
3. **Filtro por autor**: aplicar `split_own_posts` aos posts capturados antes de `_should_stop` e de `_apply_final_filters`, para que posts alheios não contem para os critérios de parada. Somar os descartados em `foreign_discarded`.
4. **Perfil indisponível**: depois da espera inicial e da leitura das estatísticas, se `profile_stats` for `None` e não houver nenhum post próprio capturado, fechar a página e levantar `ProfileUnavailable`.
5. Manter: interceptação de rede, movimento do mouse antes de rolar, pausas longas (`_LONG_PAUSE_EVERY`), parada por estagnação e por `since_date`.

### 2. Coleta de um perfil (`pipeline/collect_one.py`, novo)

`async def collect_one(cfg: PipelineConfig, paths: DataPaths, handle: str, run_id: str) -> CollectOneResult`

1. `paths.ensure()`.
2. Abrir o navegador com `playwright.chromium.launch_persistent_context(user_data_dir=paths.browser, headless=cfg.collect.headless, viewport={"width": 1280, "height": 900})`.
3. Executar `extract_posts` com `cfg.collect.max_posts` e `cfg.collect.since_date`; novas tentativas para exceções genéricas conforme `cfg.collect.retry` (não repetir para `SessionBlocked` nem `ProfileUnavailable`).
4. Em sucesso, **na ordem obrigatória**:
   1. persistir `posts.json` (merge por `shortcode`, como `_persist_posts` faz hoje), `posts.parquet`, `profile.json` (como `_write_profile_stats`) e o chunk em `raw/` (como `_persist_raw`), usando escrita atômica local. Mover essas funções para `pipeline/persistence.py` e adaptá-las para receber `DataPaths` e escrever direto no disco (sem a abstração `Storage`);
   2. acrescentar as linhas de TODO (`todo_records`, etapa 02) em `paths.todo_file(handle)`, para **todos** os posts do resultado, não apenas os novos.
5. Fechar o contexto do navegador em qualquer caso (`finally`).
6. Devolver `CollectOneResult` (dataclass): `status` (`collected`, `unavailable`, `blocked`, `error`), `collection_id`, `posts_in_period`, `media_items`, `foreign_posts_discarded`, `error`.

`collect_one` **não** escreve em `COLLECTED`: isso é responsabilidade do orquestrador (etapa 04).

### 3. Comando interno `collect-one` (`cli.py`)

`zeezazum collect-one --config <yaml> --handle <h> --run-id <id> --result-file <caminho>` com `hidden=True`.

- Configura o log em `collector.log` (via `logsetup`).
- Executa `collect_one`, grava o `CollectOneResult` como JSON em `--result-file` (escrita atômica) e sai com: 0 para `collected` e `unavailable`, 3 para `blocked`, 1 para `error`.
- Trata `SIGTERM`: cancela a coleta, fecha o navegador e sai com código 1 sem gravar TODO parcial.
- Registra no log, ao final, `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss` em MB.

### 4. Remoção do fluxo antigo

Remover: `pipeline/orchestrator.py`, `pipeline/state.py`, `platforms/instagram/downloader.py`, `storage/` inteiro, os comandos `scrape`, `backfill` e `media-sync`, `ZeezazumConfig` e as seções antigas do `config.py`, `config.example.yaml` (substituído por `config.pipeline.example.yaml`, que passa a se chamar `config.example.yaml`). Ajustar `cli.py` para que `prepare` continue funcionando. Manter `scripts/local_login.py` até a etapa 04.

## Fora do escopo

Orquestração de vários perfis, pausas entre perfis, `COLLECTED`, `login` (etapa 04).

## Testes

- `test_scraper_filters.py`: com `Post` sintéticos, `_should_stop` e `_apply_final_filters` ignoram posts alheios e respeitam `since_date` e `max_posts`.
- `test_persistence.py`: merge de `posts.json` não duplica `shortcode`; escrita atômica não deixa `.part`; `raw/` recebe um chunk por chamada só com posts novos.
- `test_collect_one.py`: substituir o scraper por um dublê (injeção de dependência: `collect_one` recebe uma fábrica de scraper opcional) e verificar:
  - sucesso grava `output/` antes do TODO e devolve `collected` (simular falha na gravação do TODO e verificar que `output/` existe e nenhum TODO foi gravado);
  - `ProfileUnavailable` devolve `unavailable` sem TODO;
  - `SessionBlocked` devolve `blocked` sem TODO;
  - TODO contém todos os itens de todos os posts do resultado.

Não há teste automatizado com navegador real.

## Validação manual no servidor (antes da revisão)

```bash
xvfb-run -a uv run zeezazum collect-one --config config.yaml --handle mathsmoreira --run-id teste --result-file /tmp/r.json; echo "saída: $?"; cat /tmp/r.json
```

Usar um perfil válido, um inexistente (ex.: `channel`) e conferir: TODO gerado só para o válido; `status` correto; nenhum post com `owner_username` diferente do handle em `posts.json`.

Observação: o perfil persistente do navegador ainda não tem login nesta etapa. Para a validação, fazer o login uma vez com um script provisório que use `launch_persistent_context` no mesmo `paths.browser` (pode ser uma adaptação local de `scripts/local_login.py`, sem commit), ou aguardar a etapa 04.

## Critérios de revisão

1. `uv run pytest` passa.
2. `uv run zeezazum --help` lista `prepare` (e não lista `collect-one`, `scrape`, `backfill`, `media-sync`).
3. Nenhum módulo fora de `platforms/instagram/` e `pipeline/collect_one.py` importa `playwright`.
4. Um commit: `etapa 03: coleta de um perfil com filtro de autoria e detecção de bloqueio`.
