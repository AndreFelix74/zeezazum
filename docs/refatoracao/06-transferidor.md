# Etapa 06: transferidor (`transfer`)

Pré-requisitos: etapas 01 a 05 concluídas e revisadas; leitura de `00-arquitetura.md`.

## Objetivo

Processo contínuo que **move** para o S3 a mídia baixada e mantém no bucket uma cópia atualizada dos metadados de cada perfil e dos registros. Mover significa: enviar, conferir no destino, remover o arquivo local e registrar em UPLOADED.

## Escopo

### 1. Destino (`remote.py`, novo)

```python
class ObjectStore(Protocol):
    def head_size(self, key: str) -> int | None: ...     # None se o objeto não existir
    def upload_file(self, local_path: Path, key: str) -> None: ...
```

- `S3ObjectStore(bucket, region)`: `head_size` com `head_object` (404 → `None`; demais erros propagam); `upload_file` com `client.upload_file` (multipart e leitura em fluxo do disco).
- **Nenhuma operação de remoção no S3.** A política IAM da instância não concede `s3:DeleteObject`, de propósito.
- Credenciais pela cadeia padrão do `boto3` (role da instância).
- Mover `boto3` de dependência opcional (`[project.optional-dependencies] s3`) para dependência obrigatória e remover o extra. Regenerar o `uv.lock`.

### 2. Laço principal (`pipeline/transfer.py`, novo)

1. Obter a trava `transfer` (código 2 se ocupada).
2. Manter em memória apenas `handle -> deslocamento lido em done/<handle>.jsonl` e o horário do último backup dos registros.
3. A cada ciclo:
   1. para cada `done/<handle>.jsonl` com linhas novas, processar o handle (seção 3);
   2. se tiverem passado `cfg.transfer.ledger_backup_interval_seconds` desde o último backup, executar o backup (seção 4);
   3. sem novidades, dormir `cfg.transfer.poll_interval_seconds`.
4. Erro do S3 em qualquer envio: registrar `ERROR`, não avançar o deslocamento daquele handle e aplicar espera crescente (30 s, dobrando até 15 min) antes do ciclo seguinte. Ao voltar a funcionar, a espera é zerada.
5. `SIGTERM`/`SIGINT`: concluir o arquivo em envio e sair com 0.

### 3. Processamento de um handle

1. Carregar `uploaded/<handle>.jsonl`: conjunto de `item_id` com `kind = media` e, para cada arquivo `meta`, o último `(bytes, local_mtime)`.
2. **Mídia**: para cada linha nova de DONE com `status = ok` cujo `item_id` não esteja em UPLOADED:
   1. `key = cfg.transfer.prefix + file`;
   2. se o arquivo local existir: `upload_file`, depois `head_size(key)`; se o tamanho no destino for igual ao local, remover o arquivo local (se `delete_local_media_after_upload`) e acrescentar a linha `media` em UPLOADED; se for diferente, registrar `ERROR` e não remover;
   3. se o arquivo local **não** existir (queda entre a remoção e o registro): `head_size(key)`; se existir com o `bytes` da linha de DONE, acrescentar a linha em UPLOADED; caso contrário, registrar `ERROR` "arquivo ausente no disco e no destino" e seguir (o reconciliador relatará).
3. **Metadados**: para `posts.json`, `posts.parquet`, `profile.json` e cada arquivo em `raw/`, enviar se não houver linha `meta` anterior ou se o tamanho ou o `mtime` mudaram; acrescentar a linha `meta` após o envio. Nunca remover metadados locais.
4. Registrar no log, por handle: itens transferidos, bytes, metadados enviados, erros.

### 4. Backup dos registros

Enviar para `<prefix>_ledger/<caminho relativo a ledger/>` cada arquivo de `ledger/` cujo tamanho ou `mtime` tenha mudado desde o último backup (controle em memória; na inicialização, enviar todos uma vez). Esse backup não gera linha em UPLOADED.

### 5. Comando `transfer` (`cli.py`)

`zeezazum transfer --config <yaml>`: configura o log em `transfer.log` e executa o laço.

## Fora do escopo

Decisões sobre falhas de download; remoção de qualquer objeto no S3.

## Testes (com `ObjectStore` falso em memória)

- Mídia `ok` é enviada, conferida, removida localmente e registrada; `posts.json` e `raw/` nunca são removidos.
- Tamanho divergente no destino: arquivo local mantido, sem linha em UPLOADED.
- Arquivo local ausente e presente no destino com o mesmo tamanho: linha em UPLOADED sem novo envio.
- `item_id` já em UPLOADED não é reenviado.
- Metadados: reenviados só quando `mtime` ou tamanho mudam.
- Falha do destino: deslocamento não avança; o item é processado no ciclo seguinte.
- `delete_local_media_after_upload: false`: arquivo mantido, linha registrada.
- Linhas `failed` de DONE são ignoradas.

## Validação manual no servidor

Com o piloto (etapas 04 e 05): `uv run zeezazum transfer --config config.yaml`; conferir com `aws s3 ls s3://<bucket>/campanha2026-v2/ --recursive --summarize` que os objetos existem, que `media/` local foi esvaziada e que `_ledger/` contém os registros.

## Critérios de revisão

1. `uv run pytest` passa.
2. `pipeline/transfer.py` e `remote.py` não importam `playwright` nem `httpx`; nenhum outro módulo importa `boto3`.
3. Busca por `delete_object` no código não encontra nada.
4. Um commit: `etapa 06: transferidor com conferência no destino e backup dos registros`.
