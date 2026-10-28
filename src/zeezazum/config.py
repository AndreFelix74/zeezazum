"""Carregamento e validação do config.yaml."""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field, field_validator


class InputConfig(BaseModel):
    path: str
    url_column: str
    id_column: str


class RetryConfig(BaseModel):
    max_attempts: int = 3
    backoff_seconds: int = 30


class ScrapingConfig(BaseModel):
    browser: Literal["chromium", "firefox"] = "chromium"
    max_posts: int | None = 60
    since_date: str | None = None
    headless: bool = True
    session_file: str = "sessions/instagram_storage_state.json"
    delay_between_profiles_seconds: tuple[float, float] = (4, 10)
    retry: RetryConfig = Field(default_factory=RetryConfig)


class LocalStorageConfig(BaseModel):
    base_path: str = "output"


class S3StorageConfig(BaseModel):
    bucket: str | None = None
    prefix: str = "zeezazum/"
    region: str = "us-east-1"


class StorageConfig(BaseModel):
    backend: Literal["local", "s3"] = "local"
    local: LocalStorageConfig = Field(default_factory=LocalStorageConfig)
    s3: S3StorageConfig = Field(default_factory=S3StorageConfig)


class MediaConfig(BaseModel):
    download_images: bool = True
    download_videos: bool = True
    max_concurrent_downloads: int = 4


class LoggingConfig(BaseModel):
    level: str = "INFO"
    file: str = "logs/zeezazum.log"


class ZeezazumConfig(BaseModel):
    platform: Literal["instagram"] = "instagram"
    input: InputConfig
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    media: MediaConfig = Field(default_factory=MediaConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_config(path: str | Path) -> ZeezazumConfig:
    raw = yaml.safe_load(Path(path).read_text())
    return ZeezazumConfig.model_validate(raw)


_SINCE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


class PipelinePathsConfig(BaseModel):
    data_dir: str


class CollectRetryConfig(BaseModel):
    max_attempts: int = Field(default=2, ge=1)
    backoff_seconds: int = Field(default=30, ge=0)


class CollectConfig(BaseModel):
    headless: bool = False
    since_date: str | None = None
    max_posts: int | None = Field(default=None, ge=1)
    delay_between_profiles_seconds: tuple[
        Annotated[float, Field(ge=0)], Annotated[float, Field(ge=0)]
    ] = (15, 40)
    profile_timeout_seconds: int = Field(default=900, ge=1)
    retry: CollectRetryConfig = Field(default_factory=CollectRetryConfig)

    @field_validator("since_date")
    @classmethod
    def _validate_since_date(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if _SINCE_DATE.fullmatch(value) is None:
            raise ValueError("since_date deve estar no formato AAAA-MM-DD")
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("since_date deve estar no formato AAAA-MM-DD") from exc
        return value

    @field_validator("delay_between_profiles_seconds")
    @classmethod
    def _validate_delay(cls, value: tuple[float, float]) -> tuple[float, float]:
        minimum, maximum = value
        if minimum > maximum:
            raise ValueError(
                "delay_between_profiles_seconds: o mínimo deve ser menor ou igual ao máximo"
            )
        return value


class DownloadConfig(BaseModel):
    download_images: bool = True
    download_videos: bool = True
    max_concurrent: int = Field(default=4, ge=1)
    request_timeout_seconds: int = Field(default=120, ge=1)
    max_attempts: int = Field(default=3, ge=1)
    poll_interval_seconds: int = Field(default=10, ge=1)


class TransferConfig(BaseModel):
    bucket: str
    prefix: str
    region: str = "sa-east-1"
    poll_interval_seconds: int = Field(default=30, ge=1)
    ledger_backup_interval_seconds: int = Field(default=600, ge=1)
    delete_local_media_after_upload: bool = True

    @field_validator("prefix")
    @classmethod
    def _normalize_prefix(cls, value: str) -> str:
        normalized = value.lstrip("/")
        if not normalized:
            return ""
        if not normalized.endswith("/"):
            normalized = f"{normalized}/"
        return normalized


class ReconcileConfig(BaseModel):
    pending_min_age_hours: int = Field(default=6, ge=0)


class PipelineLoggingConfig(BaseModel):
    level: str = "INFO"


class PipelineConfig(BaseModel):
    platform: Literal["instagram"] = "instagram"
    paths: PipelinePathsConfig
    collect: CollectConfig = Field(default_factory=CollectConfig)
    download: DownloadConfig = Field(default_factory=DownloadConfig)
    transfer: TransferConfig
    reconcile: ReconcileConfig = Field(default_factory=ReconcileConfig)
    logging: PipelineLoggingConfig = Field(default_factory=PipelineLoggingConfig)


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    raw = yaml.safe_load(Path(path).read_text())
    return PipelineConfig.model_validate(raw)
