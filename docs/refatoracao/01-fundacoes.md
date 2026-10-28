# Etapa 01: fundações

Pré-requisito: leitura de `00-arquitetura.md`.

## Objetivo

Criar os módulos de infraestrutura que todas as etapas seguintes usam, sem alterar o comportamento atual do projeto. Os comandos existentes continuam funcionando ao fim desta etapa.

## Escopo

1. **`pyproject.toml`**
   - Grupo de desenvolvimento: `[dependency-groups] dev = ["pytest>=8", "pytest-asyncio>=0.24"]`.
   - Configuração do pytest: `[tool.pytest.ini_options] testpaths = ["tests"]`, `asyncio_mode = "auto"`.
   - Não alterar dependências de produção nesta etapa. Regenerar o `uv.lock`.

2. **`src/zeezazum/paths.py`**
   - Dataclass imutável `DataPaths(data_dir: Path)` com propriedades para cada caminho da seção 3 da arquitetura: `output`, `ledger_todo`, `ledger_done`, `ledger_uploaded`, `ledger_collected`, `reports`, `browser`, `locks`, `blocked_flag`, `logs`.
   - Métodos auxiliares: `account_dir(handle) -> Path` (`output/instagram/<handle>`), `todo_file(handle)`, `done_file(handle)`, `uploaded_file(handle)`, `collected_file(run_id)`, `lock_file(name)`, `log_file(name)`.
   - `ensure() -> None`: cria todos os diretórios (`parents=True, exist_ok=True`).
   - Validação: `handle` e `run_id` não podem conter `/`, `\`, `..` nem começar com `.`; levantar `ValueError`.

3. **`src/zeezazum/ledger.py`** (seção 4.5 da arquitetura)
   - `append(path: Path, record: dict) -> None`: cria o diretório pai se necessário; acrescenta `json.dumps(record, ensure_ascii=False, default=str) + "\n"`; `flush()` e `os.fsync()`.
   - `read_all(path: Path) -> list[dict]`: arquivo inexistente devolve lista vazia; aplica as regras de tolerância da seção 4.5.
   - `read_from(path: Path, offset: int) -> tuple[list[dict], int]`: lê a partir do deslocamento, devolve só linhas completas e o deslocamento do fim da última linha completa (uma linha incompleta no fim não avança o deslocamento).
   - Utilitário de data: `utc_now_iso() -> str` e `utc_stamp() -> str` (`YYYYMMDDTHHMMSSZ`).

4. **`src/zeezazum/locks.py`** (seção 6 da arquitetura)
   - `single_instance(lock_path: Path, process_name: str)`: gerenciador de contexto com `fcntl.flock(LOCK_EX | LOCK_NB)`; em conflito, levanta `AlreadyRunning(process_name)`.
   - Função `exit_if_running(...)` ou tratamento equivalente para que os comandos da CLI convertam `AlreadyRunning` em saída com código 2 e mensagem em português.

5. **`src/zeezazum/config.py`**: acrescentar o schema da seção 8 da arquitetura como um **novo** modelo `PipelineConfig` e a função `load_pipeline_config(path)`. Não remover nem alterar `ZeezazumConfig` nesta etapa. Validações:
   - `since_date` no formato `AAAA-MM-DD` quando presente;
   - `delay_between_profiles_seconds` com mínimo ≤ máximo;
   - `transfer.prefix` normalizado para terminar em `/` e não começar com `/`.

6. **`config.pipeline.example.yaml`** na raiz, com o conteúdo da seção 8 e um bucket fictício (`meu-bucket`).

7. **`src/zeezazum/logsetup.py`**: `configure(paths: DataPaths, process_name: str, level: str) -> None`, que grava em `paths.log_file(process_name)` com o formato atual e reduz `httpx` e `botocore` para `WARNING`.

## Fora do escopo

Qualquer alteração em `cli.py`, `scraper.py`, `orchestrator.py`, `downloader.py`, `state.py` ou nos comandos existentes.

## Testes (`tests/`)

- `test_paths.py`: caminhos derivados corretos; `ensure()` cria tudo; handles inválidos levantam `ValueError`.
- `test_ledger.py`:
  - `append` seguido de `read_all` devolve os registros na ordem;
  - última linha sem `\n` é ignorada por `read_all` e não avança o deslocamento em `read_from`;
  - linha inválida no meio é ignorada com `WARNING` (usar `caplog`);
  - `read_from` em chamadas sucessivas devolve só as linhas novas.
- `test_locks.py`: segunda tentativa de obter a mesma trava levanta `AlreadyRunning`; após sair do contexto, a trava pode ser obtida de novo. Para simular dois processos, usar `multiprocessing` ou `subprocess`.
- `test_config.py`: `config.pipeline.example.yaml` é válido; `since_date` inválido e intervalo de pausa invertido são rejeitados; normalização do `prefix`.

## Critérios de revisão

1. `uv run pytest` passa.
2. `uv run zeezazum --help` continua listando os comandos atuais, sem alteração.
3. Nenhum arquivo existente foi alterado além de `pyproject.toml`, `uv.lock` e `config.py` (apenas acréscimo).
4. Um commit: `etapa 01: fundações (paths, ledger, locks, config do pipeline)`.
