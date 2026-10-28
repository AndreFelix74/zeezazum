# Etapa 08: implantação, documentação e versão

Pré-requisitos: etapas 01 a 07 concluídas e revisadas; leitura de `00-arquitetura.md`.

## Objetivo

Deixar o projeto pronto para operar no servidor e para o *pull request* ao repositório original.

## Escopo

### 1. Units do systemd (`deploy/systemd/`)

Todas para o usuário `ubuntu`, com `WorkingDirectory=/home/ubuntu/zeezazum`, `KillSignal=SIGTERM` e `TimeoutStopSec=90`.

- `zeezazum-download.service`: `ExecStart=/home/ubuntu/.local/bin/uv run zeezazum download --config config.yaml`, `Restart=on-failure`, `RestartSec=30`, `RestartPreventExitStatus=2`.
- `zeezazum-transfer.service`: idem, com `transfer`.
- `zeezazum-collect@.service` (modelo; a instância é o nome do arquivo de entrada sem extensão, em `data/lotes/`):
  `ExecStart=/usr/bin/xvfb-run -a --server-args="-screen 0 1280x900x24" /home/ubuntu/.local/bin/uv run zeezazum collect --config config.yaml --input data/lotes/%i.parquet`, `Restart=no`.

### 2. `deploy/README.md`

Procedimento operacional, em português:

1. instalação das units (`sudo cp deploy/systemd/*.service /etc/systemd/system/ && sudo systemctl daemon-reload`);
2. `sudo systemctl enable --now zeezazum-download zeezazum-transfer`;
3. login: `ssh -X` no servidor e `uv run zeezazum login --config config.yaml`;
4. início de um lote: `sudo systemctl start zeezazum-collect@lote_01`;
5. acompanhamento: `journalctl -u ... -f`, `tail -f <data_dir>/logs/collector.log`, `uv run zeezazum reconcile --config config.yaml`;
6. quando a coleta parar com código 3 (`BLOCKED`): verificar a conta no celular, refazer o login e reiniciar o lote (a retomada é automática);
7. nova tentativa: `reconcile --emit-retry` e `systemctl start zeezazum-collect@...` com o arquivo gerado copiado para `data/lotes/`;
8. nunca iniciar dois lotes ao mesmo tempo (a trava impede, com código 2).

### 3. Documentação e versão

- `README.md`: substituir o fluxo antigo pelo novo (arquitetura em quatro processos, configuração, comandos, layout de diretórios, operação no servidor com referência a `deploy/README.md`). Atualizar a seção de escala com os números medidos: cerca de 58 s e 545 MB por perfil no período de campanha.
- `CHANGELOG.md`: entrada 0.3.0 listando o que foi removido (`scrape`, `backfill`, `media-sync`, `storage.backend: s3`, `scripts/local_login.py`, estado por candidato), o que foi acrescentado e as correções (autoria, bloqueio, `channel`, memória, perda de estado).
- `pyproject.toml`: versão `0.3.0`; conferir que as dependências refletem o uso real; regenerar o `uv.lock`.
- `.gitignore`: acrescentar `data_dir` de exemplo, se usado localmente, e `reports/`.
- Remover `docs/refatoracao/` ou mantê-la como registro de decisão (decisão do revisor).

## Critérios de revisão

1. `uv run pytest` passa.
2. `uv run zeezazum --help` lista exatamente `prepare`, `login`, `collect`, `download`, `transfer`, `reconcile`.
3. `systemd-analyze verify deploy/systemd/*.service` sem erros (no servidor).
4. Um commit: `etapa 08: implantação systemd, documentação e versão 0.3.0`.

## Aceitação final da refatoração (no servidor, com `data/piloto.parquet`)

1. Download e transferência ativos pelo systemd; coleta do piloto pelo `zeezazum-collect@piloto`.
2. Ao final: 9 linhas em COLLECTED; nenhuma linha de TODO sem resposta em DONE; todos os itens `ok` em UPLOADED; `media/` local vazia; objetos no bucket com os mesmos tamanhos.
3. Nenhum post em `posts.json` com `owner_username` diferente do handle, exceto coautoria.
4. `kill -9` em cada um dos três processos durante a execução, seguido de reinício: nenhum item duplicado em DONE ou UPLOADED, nenhum `.part` remanescente, nenhum item perdido segundo o `reconcile`.
5. `reconcile` sem itens faltantes; `--emit-retry` gera lista vazia.
