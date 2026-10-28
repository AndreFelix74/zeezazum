# Etapa 05: baixador (`download`)

Pré-requisitos: etapas 01 a 04 concluídas e revisadas; leitura de `00-arquitetura.md`.

## Objetivo

Processo contínuo que acompanha os arquivos de TODO, baixa cada item uma vez e registra o resultado em DONE. Não interpreta falhas, não altera TODO e não conhece o S3 nem o navegador.

## Escopo

### 1. Laço principal (`pipeline/downloader.py`, novo)

1. Obter a trava `download` (código 2 se ocupada).
2. Manter em memória apenas um dicionário `handle -> deslocamento lido em todo/<handle>.jsonl`. Na inicialização, todos os deslocamentos começam em 0.
3. A cada ciclo:
   1. listar `ledger/todo/*.jsonl` e, para cada arquivo cujo tamanho seja maior que o deslocamento registrado, ler as linhas novas com `ledger.read_from`;
   2. para cada handle com linhas novas, processar o lote do handle (seção 2) e atualizar o deslocamento;
   3. se nenhum arquivo tiver novidades, dormir `cfg.download.poll_interval_seconds`.
4. `SIGTERM`/`SIGINT`: terminar os downloads em andamento do handle atual, registrar seus resultados e sair com 0. Downloads interrompidos não registram DONE e têm o `.part` removido.

### 2. Processamento de um handle

1. Carregar `done/<handle>.jsonl` e montar dois conjuntos: `todo_ids` já respondidos e `item_ids` com `status = ok`. Descartá-los ao terminar o handle.
2. Filtrar as linhas novas pelas regras de idempotência (seção 5 da arquitetura) e por `download_images`/`download_videos` (itens desabilitados são pulados sem registro).
3. Baixar os itens restantes com um `httpx.AsyncClient` criado para o handle (`timeout=cfg.download.request_timeout_seconds`, `follow_redirects=True`) e fechado ao fim.
4. Concorrência: `asyncio.Semaphore(cfg.download.max_concurrent)` envolvendo **todo** o ciclo de um item (requisição, gravação e renomeação). Tarefas criadas e aguardadas com `asyncio.gather` dentro do handle; nada sobrevive entre handles.
5. Por item:
   1. se já existir arquivo final para `dest_stem` com extensão `.jpg`, `.png` ou `.mp4`, registrar `ok` com o arquivo existente, `bytes` do disco e `attempts = 0`;
   2. baixar com `client.stream("GET", media_url)`, gravando em blocos de 1 MiB em `<destino>.part` (escrita via `asyncio.to_thread` ou `aiofiles`); **nunca** usar `response.content`;
   3. extensão: `.mp4` para `kind = video`; para imagens, `.png` se o `content-type` contiver `png`, senão `.jpg` (regra atual de `_extension_for`);
   4. ao concluir, `os.replace` para o nome final e registrar `ok` com `http_status`, `file` (relativo a `output/`), `bytes` e `attempts`;
   5. novas tentativas imediatas (mecanismo, não política): até `cfg.download.max_attempts` para HTTP 429, 5xx, timeout e erros de conexão, com espera exponencial curta (`tenacity`, 2 s a 30 s). HTTP 4xx diferentes de 429 não são repetidos;
   6. falha final: remover o `.part` e registrar `failed` com `http_status` (ou `null`) e `error` (tipo e mensagem curta da exceção).
6. Registrar no log, por handle: itens processados, ok, falhas por código HTTP, bytes e duração.

### 3. Comando `download` (`cli.py`)

`zeezazum download --config <yaml>`: configura o log em `download.log` e executa o laço.

## Fora do escopo

Qualquer decisão sobre falhas, transferência ao S3, remoção de arquivos.

## Testes (`httpx.MockTransport`, sem rede)

- Sucesso: arquivo final existe, sem `.part`; linha `ok` com `bytes` correto; imagem PNG recebe `.png`.
- 403 e 404: uma tentativa só; linha `failed` com o código; sem `.part`.
- 503 duas vezes e 200 na terceira: linha `ok` com `attempts = 3`.
- Timeout em todas as tentativas: `failed` com `http_status = null`.
- Idempotência: linha de TODO com `todo_id` já respondido não gera nova linha; `item_id` já `ok` com outro `todo_id` é pulado sem registro; arquivo existente em disco gera `ok` com `attempts = 0`.
- Linhas acrescentadas ao TODO depois do primeiro ciclo são processadas no ciclo seguinte; linha incompleta no fim só é processada depois de completada.
- Gravação em blocos: um corpo de 5 MiB é escrito em várias chamadas (verificar com um dublê de escrita ou medindo que `response.content` não é acessado).
- `download_videos: false`: vídeos pulados sem registro.

## Validação manual no servidor

Com o TODO do piloto (etapa 04): `uv run zeezazum download --config config.yaml` por alguns minutos; conferir que cada linha de TODO tem resposta em DONE, que os arquivos existem em `media/` e que o uso de memória do processo (`ps -o rss= -p <pid>`) não ultrapassa 500 MB.

## Critérios de revisão

1. `uv run pytest` passa.
2. `pipeline/downloader.py` não importa `playwright` nem `boto3`.
3. Um commit: `etapa 05: baixador com gravação em blocos e registro DONE`.
