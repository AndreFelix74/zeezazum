# Etapa 04: orquestrador de coleta (`collect`) e `login`

Pré-requisitos: etapas 01 a 03 concluídas e revisadas; leitura de `00-arquitetura.md`.

## Objetivo

Coletar uma lista de perfis executando **um processo filho por perfil**, com retomada, pausas, interrupção ordenada e parada imediata quando a sessão cair. Criar o comando de login no perfil persistente.

## Escopo

### 1. Orquestrador (`pipeline/collector.py`, novo)

`def run_collect(cfg, paths, input_path: Path, run_id: str) -> int` (devolve o código de saída). **Não importa `playwright`.**

1. Obter a trava `collector` (código 2 se ocupada).
2. Se `paths.blocked_flag` existir, registrar `CRITICAL` e devolver 3 sem coletar.
3. Ler o parquet de entrada; exigir a coluna `handle`; normalizar para minúsculas, remover vazios e duplicatas preservando a ordem. Colunas extras são ignoradas.
4. Ler `paths.collected_file(run_id)` e remover da lista os handles com `status` `collected` ou `unavailable` (retomada).
5. Para cada handle restante:
   1. a partir do segundo, aguardar uma pausa aleatória em `cfg.collect.delay_between_profiles_seconds` (interrompível por sinal);
   2. executar `[sys.executable, "-m", "zeezazum.cli", "collect-one", "--config", ..., "--handle", h, "--run-id", run_id, "--result-file", <arquivo temporário em paths.locks ou tempfile>]` com `subprocess.Popen` e esperar com `timeout=cfg.collect.profile_timeout_seconds`;
   3. em timeout: `terminate()`, esperar 30 s, `kill()` se necessário; resultado `error` com `error="timeout"`;
   4. ler o arquivo de resultado (se ausente ou inválido: `error` com o código de saída);
   5. acrescentar a linha de COLLECTED (formato 4.4), com `exit_code`, horários e `max_rss_mb` (diferença de `resource.getrusage(RUSAGE_CHILDREN).ru_maxrss` antes e depois, ou o valor registrado pelo filho);
   6. se o resultado for `blocked`: criar `paths.blocked_flag` com data, handle e erro; registrar `CRITICAL`; encerrar o laço e devolver 3.
6. Registrar no log, por perfil, uma linha `INFO` com handle, status, posts no período, itens de mídia, descartados, duração e memória.
7. Ao final, registrar um resumo da execução (contagem por status) e devolver 0.

Sinais: `SIGTERM` e `SIGINT` no orquestrador encaminham `SIGTERM` ao filho em andamento, esperam até 30 s, registram a linha de COLLECTED como `error` com `error="interrompido"` e devolvem 0. Nenhum novo filho é iniciado depois do sinal.

### 2. Comando `collect` (`cli.py`)

`zeezazum collect --config <yaml> --input <parquet> [--run-id <id>]`. `run_id` padrão: nome do arquivo de entrada sem extensão. Configura o log em `collector.log` e chama `run_collect`, saindo com o código devolvido.

### 3. Comando `login` (`cli.py` + `platforms/instagram/login.py`)

`zeezazum login --config <yaml>`:

1. Obter a trava `collector` (o perfil persistente não pode estar aberto pela coleta).
2. `launch_persistent_context(user_data_dir=paths.browser, headless=False)`, abrir `https://www.instagram.com/accounts/login/`.
3. Mostrar no terminal: "Faça login na janela do navegador, abra um perfil para confirmar que os posts carregam e pressione Enter aqui."
4. Após o Enter, navegar para `https://www.instagram.com/` e verificar com `is_blocking_url(page.url)`; se ainda estiver bloqueado, avisar e sair com código 3 sem remover `BLOCKED`.
5. Caso contrário, fechar o contexto, remover `paths.blocked_flag` se existir e sair com 0.

Remover `scripts/local_login.py`.

## Fora do escopo

Download, transferência, relatório.

## Testes

Usar um filho falso: o orquestrador recebe, para testes, o comando a executar (parâmetro opcional `child_cmd_factory`), apontando para um script Python em `tests/fixtures/` que lê os argumentos e escreve um resultado predefinido conforme o handle.

- Retomada: com COLLECTED prévio contendo `a: collected`, `b: unavailable`, `c: error`, a execução processa apenas `c` e os demais handles novos.
- Bloqueio: o terceiro handle devolve `blocked`; o quarto não é executado; `BLOCKED` existe; código 3.
- `BLOCKED` prévio: nenhum filho executado; código 3.
- Timeout: filho que dorme além do limite é encerrado; linha `error` com `timeout`; o laço continua.
- Resultado ausente: filho que sai com 1 sem escrever resultado gera linha `error`.
- Pausas: com `delay_between_profiles_seconds: [0, 0]`, nenhuma espera; garantir que a pausa não ocorre antes do primeiro handle.
- Entrada: duplicatas e maiúsculas normalizadas; parquet sem coluna `handle` gera erro claro.

## Validação manual no servidor

1. `zeezazum login` via `ssh -X` e login com a conta de coleta.
2. `xvfb-run -a uv run zeezazum collect --config config.yaml --input data/piloto.parquet`.
3. Conferir: 9 linhas em `ledger/collected/piloto.jsonl`; TODO para os perfis válidos; `max_rss_mb` estável entre perfis; nenhum post alheio em `posts.json` (`owner_username`).
4. Interromper com Ctrl+C no meio, rodar de novo e verificar que só os perfis restantes são coletados.

## Critérios de revisão

1. `uv run pytest` passa.
2. `uv run zeezazum --help` lista `prepare`, `login`, `collect`.
3. `pipeline/collector.py` não importa `playwright`.
4. Um commit: `etapa 04: orquestrador de coleta por processo e login no perfil persistente`.
