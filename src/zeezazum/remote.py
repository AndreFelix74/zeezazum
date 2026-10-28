"""Destino dos arquivos transferidos. Não remove objetos remotos."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import boto3
from botocore.exceptions import ClientError

_MISSING = {"404", "NoSuchKey", "NotFound"}


class ObjectStore(Protocol):
    """Operações usadas pelo transferidor. Nenhuma delas apaga um objeto."""

    def head_size(self, key: str) -> int | None:
        """Tamanho do objeto, ou None se ele não existir."""

    def upload_file(self, local_path: Path, key: str) -> None:
        """Envia o arquivo local. A leitura fica a cargo da implementação."""


class S3ObjectStore:
    """Bucket S3 com a cadeia de credenciais padrão do boto3."""

    def __init__(self, bucket: str, region: str, *, client: Any | None = None) -> None:
        self._bucket = bucket
        if client is None:
            client = boto3.client("s3", region_name=region)
        self._client = client

    def head_size(self, key: str) -> int | None:
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            error = exc.response.get("Error", {})
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if str(error.get("Code", "")) in _MISSING or status == 404:
                return None
            raise
        return int(response["ContentLength"])

    def upload_file(self, local_path: Path, key: str) -> None:
        self._client.upload_file(str(local_path), self._bucket, key)
