# Refatoração do Zeezazum: arquitetura e contratos

Documento de referência para todas as etapas (`01` a `08`). Fork `AndreFelix74/zeezazum`, branch `refatoracao-pipeline`, a partir do commit `2faba60` (release 0.2.1). Resultado final: versão 0.3.0.

## Como usar estes documentos no Cursor

1. Coloque a pasta `docs/refatoracao/` no repositório.
2. Em cada etapa, peça ao Cursor: "Leia `docs/refatoracao/00-arquitetura.md` e implemente **somente** `docs/refatoracao/0N-....md`."
3. Cada etapa termina com testes passando e um commit. **Não avance para a etapa seguinte sem revisão humana do diff.**
4. Em caso de conflito entre uma etapa e este documento, este documento prevalece. Se algo estiver ambíguo, o Cursor deve perguntar, não decidir.

## 1. Problemas que motivam a refatoração

Observados em produção (EC2 m7i-flex.large, 8 GB, 17.638 perfis):

1. **Morte por falta de memória** após 785 perfis (`anon-rss: 5,1 GB`). Causas em `platforms/instagram/downloader.py`: a lista `pending` retém todas as tarefas até o fim da execução; o semáforo não cobre a gravação; `response.content` carrega vídeos inteiros na memória.
2. **Estado perdido**: com `storage.backend: s3`, o `_state.duckdb` fica em `/tmp` e só é enviado no `finally`; um `SIGKILL` perde o progresso.
3. **Contaminação**: 13,5% das contas receberam posts sugeridos de outros perfis, porque o fallback genérico de `parser.py` captura qualquer post e nada confere o autor.
4. **Tela de verificação não detectada**: só `/accounts/login` é reconhecido; `/challenge/` vira "nenhum post novo" com status `done`.
5. **Estado por candidato**: um perfil citado por 91 candidatos é visitado 91 vezes.
6. **`channel` interpretado como handle** em `input/url_parser.py`.
7. **Responsabilidades misturadas**: coleta, download e persistência remota no mesmo processo e na mesma tarefa.

## 2. Arquitetura

Quatro processos independentes. Eles não se chamam e não compartilham memória nem banco de dados: comunicam-se apenas por **registros só-acréscimo** (arquivos JSONL) em disco.

| Processo | Comando | Lê | Escreve | Execução |
|---|---|---|---|---|
| Coletor | `collect` | lista de handles (parquet) | `output/`, `TODO`, `COLLECTED` | por lote, sob demanda |
| Baixador | `download` | `TODO`, `DONE` | mídia em `output/`, `DONE` | contínua |
| Transferidor | `transfer` | `DONE`, `UPLOADED` | objetos no S3, `UPLOADED`; remove mídia local transferida | contínua |
| Reconciliador | `reconcile` | todos os registros | relatório; opcionalmente lista de nova tentativa | sob demanda, somente leitura |

Fluxo:

```
lote.parquet ─► collect ─► TODO ─► download ─► DONE ─► transfer ─► UPLOADED

reconcile ─► relatório  [+ retry_<data>.parquet com coluna handle, mesmo formato dos lotes]
                                  │
                                  └─► decisão humana ─► collect --input retry_<data>.parquet
```

Princípios obrigatórios:

- **Execução otimista**: nenhum processo decide sobre falhas de outro. O baixador e o transferidor executam e registram o resultado. Só o reconciliador interpreta resultados, e ele apenas relata; a decisão de reprocessar é humana.
- **Um único tipo de entrada por processo.** O coletor só conhece listas de handles; ele não sabe se a lista é um lote original ou uma nova tentativa.
- **Um escritor por arquivo de registro.** Nenhum arquivo é escrito por dois processos.
- **Idempotência mecânica**: reprocessar uma entrada já processada não produz efeito duplicado (regras na seção 5).
- **Instância única por processo**, com `fcntl.flock` (seção 6).
- **Escrita atômica** de arquivos finais: gravar `<nome>.part` e `os.replace`. Nenhum processo lê ou envia `.part`.
- **Memória limitada por construção**: nenhum arquivo de mídia inteiro em memória; nenhuma estrutura cresce com o total de perfis processados.
- **Isolamento de dependências**: só o coletor (e o comando `login`) importa `playwright`; só o baixador importa `httpx`; só o transferidor importa `boto3`; o reconciliador não usa rede.

## 3. Layout de diretórios

Tudo sob `paths.data_dir` (ex.: `/data/zeezazum`). O módulo `src/zeezazum/paths.py` (etapa 01) é o **único** lugar que monta caminhos.

```
<data_dir>/
├── output/instagram/<handle>/
│   ├── posts.json
│   ├── posts.parquet
│   ├── profile.json
│   ├── raw/<timestamp>.jsonl.gz
│   └── media/<shortcode>_<index>.<ext>     # removida após transferência
├── ledger/
│   ├── todo/<handle>.jsonl                 # escritor: coletor
│   ├── done/<handle>.jsonl                 # escritor: baixador
│   ├── uploaded/<handle>.jsonl             # escritor: transferidor
│   └── collected/<run_id>.jsonl            # escritor: coletor (processo orquestrador)
├── reports/                                # escritor: reconciliador
├── browser/                                # perfil persistente do Chromium
├── locks/                                  # collector.lock, download.lock, transfer.lock
├── BLOCKED                                 # existe quando a sessão do Instagram caiu
└── logs/                                   # collector.log, download.log, transfer.log, reconcile.log
```

## 4. Formatos dos registros

Uma linha JSON por evento, codificação UTF-8, terminada em `\n`. Datas em ISO 8601 UTC com fuso (`2026-10-28T13:05:01+00:00`).

Identificadores:

- `item_id` = `<shortcode>_<index>` (ex.: `DfApM5ooxyv_0`). Identifica um arquivo de mídia, estável entre coletas.
- `collection_id` = `<handle>@<YYYYMMDDTHHMMSSZ>`. Uma coleta de um perfil.
- `todo_id` = `<item_id>@<YYYYMMDDTHHMMSSZ>` (o mesmo carimbo do `collection_id`). Uma linha de TODO.

### 4.1 TODO (`ledger/todo/<handle>.jsonl`)

Uma linha por item de mídia de **todo post encontrado no período** naquela coleta (não apenas dos posts novos):

```json
{"todo_id": "DfApM5ooxyv_0@20261028T130501Z", "item_id": "DfApM5ooxyv_0", "collection_id": "fulano@20261028T130501Z", "run_id": "lote_01", "handle": "fulano", "shortcode": "DfApM5ooxyv", "index": 0, "kind": "video", "media_url": "https://scontent...", "post_url": "https://www.instagram.com/p/DfApM5ooxyv/", "dest_stem": "instagram/fulano/media/DfApM5ooxyv_0", "collected_at": "2026-10-28T13:05:01+00:00"}
```

`dest_stem` é relativo a `output/`, sem extensão.

### 4.2 DONE (`ledger/done/<handle>.jsonl`)

Uma linha por linha de TODO processada:

```json
{"todo_id": "DfApM5ooxyv_0@20261028T130501Z", "item_id": "DfApM5ooxyv_0", "status": "ok", "http_status": 200, "error": null, "file": "instagram/fulano/media/DfApM5ooxyv_0.mp4", "bytes": 10485760, "attempts": 1, "finished_at": "2026-10-28T13:06:10+00:00"}
```

- `status`: `ok` ou `failed`. Não existe "expirado": o baixador registra o fato (`http_status`, `error`), não a interpretação.
- Em `failed`, `file` e `bytes` são `null`; `http_status` é `null` quando não houve resposta (timeout, conexão).

### 4.3 UPLOADED (`ledger/uploaded/<handle>.jsonl`)

```json
{"kind": "media", "item_id": "DfApM5ooxyv_0", "file": "instagram/fulano/media/DfApM5ooxyv_0.mp4", "key": "campanha2026-v2/instagram/fulano/media/DfApM5ooxyv_0.mp4", "bytes": 10485760, "local_mtime": null, "uploaded_at": "2026-10-28T13:07:00+00:00"}
{"kind": "meta", "item_id": null, "file": "instagram/fulano/posts.json", "key": "campanha2026-v2/instagram/fulano/posts.json", "bytes": 48211, "local_mtime": 1761656701.42, "uploaded_at": "2026-10-28T13:07:01+00:00"}
```

### 4.4 COLLECTED (`ledger/collected/<run_id>.jsonl`)

Uma linha por perfil processado na execução `run_id` (por padrão, o nome do arquivo de entrada sem extensão):

```json
{"run_id": "lote_01", "handle": "fulano", "collection_id": "fulano@20261028T130501Z", "status": "collected", "since_date": "2026-08-15", "posts_in_period": 55, "media_items": 92, "foreign_posts_discarded": 0, "error": null, "exit_code": 0, "started_at": "...", "finished_at": "...", "max_rss_mb": 310}
```

`status`: `collected`, `unavailable`, `error` ou `blocked`.

### 4.5 Leitura e escrita dos registros (`src/zeezazum/ledger.py`, etapa 01)

- Escrita: abrir em modo acréscimo, escrever a linha com `\n`, `flush()` e `os.fsync()`.
- Leitura: ignorar a última linha se ela não terminar em `\n` ou não for JSON válido (escrita interrompida). Linhas inválidas no meio do arquivo geram `WARNING` com arquivo e número da linha e são ignoradas.
- Leitura incremental: função que recebe um deslocamento em bytes e devolve as linhas completas novas e o novo deslocamento.

## 5. Regras de idempotência

- **Coletor**: ao retomar uma execução (`run_id` já existente), pula handles com linha `collected` ou `unavailable` em `collected/<run_id>.jsonl`. Ordem por perfil: gravar `output/` → acrescentar as linhas de TODO → acrescentar a linha de COLLECTED. Uma queda antes do COLLECTED repete a coleta do perfil; as linhas de TODO duplicadas são inofensivas pela regra do baixador.
- **Baixador**: pula uma linha de TODO se o `todo_id` já tiver resposta em DONE **ou** se o `item_id` já tiver uma linha `ok` em DONE. No segundo caso, não escreve nada.
- **Transferidor**: pula uma linha `ok` de DONE se o `item_id` já tiver linha `media` em UPLOADED. Arquivos `meta` são reenviados quando `local_mtime` ou o tamanho mudarem em relação à última linha `meta` daquele arquivo.

## 6. Travas

`src/zeezazum/locks.py` (etapa 01): gerenciador de contexto `single_instance(path)` com `fcntl.flock(LOCK_EX | LOCK_NB)`. Se a trava estiver ocupada, o comando sai com código 2 e a mensagem "outra instância de <processo> está em execução".

| Trava | Usada por |
|---|---|
| `locks/collector.lock` | `collect` e `login` (o perfil persistente do navegador não pode ser aberto por dois processos) |
| `locks/download.lock` | `download` |
| `locks/transfer.lock` | `transfer` |

O `reconcile` não usa trava: é somente leitura.

## 7. Códigos de saída

| Código | Significado |
|---|---|
| 0 | sucesso, ou interrupção ordenada por `SIGTERM`/`SIGINT` |
| 2 | outra instância em execução |
| 3 | sessão do Instagram bloqueada (`BLOCKED` criado) |
| 1 | qualquer outro erro |

## 8. Configuração (schema final)

```yaml
platform: instagram

paths:
  data_dir: /data/zeezazum

collect:
  headless: false                  # no servidor, rodar sob xvfb-run
  since_date: "2026-08-15"
  max_posts: null
  delay_between_profiles_seconds: [15, 40]
  profile_timeout_seconds: 900
  retry:
    max_attempts: 2
    backoff_seconds: 30

download:
  download_images: true
  download_videos: true
  max_concurrent: 4
  request_timeout_seconds: 120
  max_attempts: 3
  poll_interval_seconds: 10

transfer:
  bucket: scraper-insta-elections-389425628530-sa-east-1-an
  prefix: campanha2026-v2/
  region: sa-east-1
  poll_interval_seconds: 30
  ledger_backup_interval_seconds: 600
  delete_local_media_after_upload: true

reconcile:
  pending_min_age_hours: 6         # TODO sem DONE há menos tempo que isto não conta como falta

logging:
  level: INFO
```

## 9. CLI final

| Comando | Etapa |
|---|---|
| `prepare` | inalterado |
| `login` | 04 |
| `collect --input <parquet> [--run-id <id>]` | 04 |
| `collect-one ...` (oculto, uso interno) | 03 |
| `download` | 05 |
| `transfer` | 06 |
| `reconcile [--input <parquet> ...] [--emit-retry]` | 07 |

Removidos na etapa 03: `scrape`, `backfill`, `media-sync`.

## 10. Convenções

- Identificadores, comandos, chaves de configuração e nomes de diretório em inglês, como o código atual. Comentários, docstrings, mensagens de log e textos da CLI em português.
- Python ≥ 3.11, tipagem em todas as funções públicas.
- Testes em `tests/`, sem acesso à rede, com `pytest` e `pytest-asyncio`.
- Cada processo configura o próprio log em `<data_dir>/logs/<processo>.log`, com o formato atual (`%(asctime)s %(levelname)s %(name)s: %(message)s`). Manter `httpx` e `botocore` em `WARNING`.
