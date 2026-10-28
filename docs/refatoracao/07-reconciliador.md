# Etapa 07: reconciliador (`reconcile`)

Pré-requisitos: etapas 01 a 06 concluídas e revisadas; leitura de `00-arquitetura.md`.

## Objetivo

Comando sob demanda, **somente leitura** em relação aos registros, que compara o previsto (TODO) com o realizado (DONE, UPLOADED), produz um relatório e, opcionalmente, a lista de perfis para nova coleta. Não move, não apaga e não altera nenhum arquivo além dos que cria em `reports/`. Pode rodar com os outros processos em execução.

## Escopo

### 1. Situação de cada item (`pipeline/reconcile.py`, novo)

Para cada handle com arquivo de TODO, calcular a situação de cada `item_id`:

| Situação | Regra |
|---|---|
| `uploaded` | existe linha `media` em UPLOADED |
| `downloaded` | última linha de DONE é `ok` e não há UPLOADED |
| `failed` | última linha de DONE é `failed` |
| `pending` | nenhuma linha de DONE para o `todo_id` mais recente do item |
| `removed` | o item não aparece na coleta mais recente do perfil (`collection_id` mais novo), mas apareceu em uma anterior, e não está `uploaded` |

- "Mais recente" segue a ordem do carimbo no `todo_id`/`collection_id`.
- `pending` só conta como falta se o `collected_at` da linha tiver mais de `cfg.reconcile.pending_min_age_hours`; abaixo disso, a situação é `in_progress`.

Para cada handle, a situação da coleta vem da linha mais recente em qualquer `ledger/collected/*.jsonl`: `collected`, `unavailable`, `error`, `blocked`. Se `--input` for informado (um ou mais parquets com coluna `handle`), handles da entrada sem nenhuma linha em COLLECTED aparecem como `not_collected`.

### 2. Relatório

Gravar em `reports/reconcile_<YYYYMMDDTHHMMSSZ>.md` e imprimir um resumo no terminal:

1. Totais: perfis por situação de coleta; itens por situação; bytes transferidos.
2. Falhas por `http_status` (incluindo `null`), com contagem de itens e de perfis.
3. Perfis com itens faltantes (`failed` ou `pending`), ordenados pela quantidade de itens faltantes.
4. Itens com mais de uma linha `failed` (falha recorrente), com `post_url`.
5. Perfis `error`, `blocked` e `not_collected`.
6. Itens `removed` (contagem por perfil).

Gravar também `reports/reconcile_<carimbo>_items.csv` com uma linha por item faltante: `handle, item_id, post_url, situation, http_status, attempts, failures, last_collected_at`.

### 3. Lista de nova tentativa (`--emit-retry`)

Gravar `reports/retry_<carimbo>.parquet` com a coluna `handle` (e a coluna informativa `reason`), contendo os perfis:

- com pelo menos um item `failed` ou `pending` fora do intervalo `in_progress`;
- ou com última situação de coleta `error`.

Excluir perfis `unavailable` e itens `removed`. O arquivo tem o **mesmo formato dos lotes**: o coletor o recebe com `--input` sem saber que é uma nova tentativa. Imprimir no terminal o comando sugerido:

```
zeezazum collect --config config.yaml --input reports/retry_<carimbo>.parquet
```

O reconciliador **não** executa esse comando.

### 4. Comando `reconcile` (`cli.py`)

`zeezazum reconcile --config <yaml> [--input <parquet> ...] [--emit-retry]`: configura o log em `reconcile.log`. Não usa trava.

### 5. Memória

Processar um handle por vez (ler os três registros daquele handle, calcular, acumular apenas contadores e as linhas do CSV). Gravar o CSV em fluxo, não montá-lo inteiro em memória.

## Fora do escopo

Executar novas coletas, alterar registros, qualquer acesso à rede.

## Testes

Montar registros sintéticos em um `tmp_path` e verificar:

- cada situação da tabela da seção 1, incluindo `removed` (item presente na coleta 1 e ausente na coleta 2) e `in_progress` (TODO recente sem DONE);
- item com `failed` seguido de `ok` em outro `todo_id` é `downloaded`, não `failed`;
- `--emit-retry` inclui perfis com falha e com coleta `error`; exclui `unavailable` e perfis cujos únicos faltantes são `removed`;
- `--input` com handle não coletado gera `not_collected`;
- arquivos de registro com última linha incompleta não quebram o cálculo;
- o reconciliador não altera nenhum arquivo fora de `reports/` (comparar `mtime` e tamanho de `ledger/` antes e depois).

## Validação manual no servidor

Com o piloto completo (etapas 04 a 06), rodar `uv run zeezazum reconcile --config config.yaml --input data/piloto.parquet --emit-retry` e conferir que o relatório bate com o que se vê no bucket e no disco. Para provocar falhas, editar uma cópia de um arquivo de TODO com uma `media_url` inválida, rodar o baixador e conferir que o item aparece como `failed` e o perfil entra na lista de nova tentativa.

## Critérios de revisão

1. `uv run pytest` passa.
2. `pipeline/reconcile.py` não importa `playwright`, `httpx` nem `boto3`.
3. Um commit: `etapa 07: reconciliador somente leitura com relatório e lista de nova tentativa`.
