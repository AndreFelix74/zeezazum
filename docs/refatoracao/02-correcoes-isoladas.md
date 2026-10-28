# Etapa 02: correções isoladas (funções puras)

Pré-requisitos: etapa 01 concluída e revisada; leitura de `00-arquitetura.md`.

## Objetivo

Implementar e testar as correções de lógica que não dependem de navegador nem de rede, para que a etapa 03 apenas as conecte.

## Escopo

1. **`input/url_parser.py`**
   - Acrescentar a `_NON_PROFILE_SEGMENTS`: `channel`, `direct`, `s`, `share`, `about`, `legal`, `web`, `developer`.
   - Atualizar o comentário explicando que são caminhos do Instagram que não correspondem a perfis.

2. **`platforms/base.py`**
   - Manter `ProfileUnavailable`, com docstring restrita a "perfil inexistente, removido ou privado".
   - Criar `SessionBlocked(Exception)`: "login exigido, verificação de segurança ou conta suspensa; a coleta inteira deve parar".

3. **`platforms/instagram/ownership.py`** (novo)
   - `is_own_post(post: Post, handle: str) -> bool`:
     - `True` se `post.metrics.get("owner_username")`, em minúsculas, for igual a `handle` em minúsculas;
     - `True` se `handle` em minúsculas estiver em `[c.lower() for c in post.metrics.get("coauthors") or [] if c]`;
     - `False` em qualquer outro caso, inclusive dono ausente ou `None`.
   - `split_own_posts(posts, handle) -> tuple[list[Post], int]`: devolve os posts próprios e a quantidade descartada.

4. **`platforms/instagram/blocking.py`** (novo)
   - Constante `BLOCKING_URL_MARKERS = ("/accounts/login", "/challenge/", "/accounts/suspended", "/accounts/disabled")`.
   - `is_blocking_url(url: str) -> bool`.

5. **`platforms/instagram/media_items.py`** (novo)
   - `todo_records(posts, *, handle, run_id, collection_stamp, collected_at) -> list[dict]`: gera uma linha de TODO (formato 4.1 da arquitetura) para cada `MediaItem` de cada post, na ordem dos posts e dos índices.
   - `item_id = f"{shortcode}_{index}"`; `todo_id = f"{item_id}@{collection_stamp}"`; `dest_stem = f"instagram/{handle}/media/{shortcode}_{index}"`.

## Fora do escopo

Conectar essas funções ao scraper ou ao orquestrador (etapa 03).

## Testes

- `test_url_parser.py`: URLs com `instagram.com/channel/...`, `/direct/...`, `/s/...`, `/share/...` não geram contas; URLs de perfil, `@handle` e os prefixos "Link Instagram:" continuam funcionando (cobrir os casos descritos nos comentários atuais do módulo).
- `test_ownership.py`: dono igual (com diferença de maiúsculas); coautor; outro autor; dono ausente; `coauthors` com `None`; `split_own_posts` conta os descartados.
- `test_blocking.py`: cada marcador é detectado; URL de perfil e de post não são.
- `test_media_items.py`: post com carrossel de 3 itens gera 3 linhas com índices 0, 1 e 2; identificadores no formato exato da arquitetura; `kind` com os valores `image`/`video`.

Construir os `Post` dos testes diretamente pela dataclass de `models.py`, sem payloads reais.

## Critérios de revisão

1. `uv run pytest` passa.
2. Os comandos atuais continuam funcionando (`uv run zeezazum --help`).
3. Um commit: `etapa 02: correções isoladas (parser de URL, autoria, bloqueio, itens de mídia)`.
