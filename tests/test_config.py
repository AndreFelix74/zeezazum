"""Configuração do pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from zeezazum.config import load_pipeline_config

ROOT = Path(__file__).resolve().parents[1]


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_example_is_valid() -> None:
    cfg = load_pipeline_config(ROOT / "config.example.yaml")
    assert cfg.platform == "instagram"
    assert cfg.paths.data_dir == "/data/zeezazum"
    assert cfg.collect.since_date == "2026-08-15"
    assert cfg.collect.headless is False
    assert cfg.collect.delay_between_profiles_seconds == (15, 40)
    assert cfg.collect.retry.max_attempts == 2
    assert cfg.download.max_concurrent == 4
    assert cfg.transfer.bucket == "meu-bucket"
    assert cfg.transfer.prefix == "campanha2026-v2/"
    assert cfg.transfer.region == "sa-east-1"
    assert cfg.reconcile.pending_min_age_hours == 6
    assert cfg.logging.level == "INFO"


@pytest.mark.parametrize("since_date", ["15/08/2026", "2026-8-15", "2026-02-31", "20260815"])
def test_invalid_since_date_is_rejected(tmp_path: Path, since_date: str) -> None:
    path = _write(
        tmp_path,
        f"""\
platform: instagram
paths:
  data_dir: /data/zeezazum
collect:
  since_date: "{since_date}"
transfer:
  bucket: meu-bucket
  prefix: campanha/
""",
    )
    with pytest.raises(ValidationError):
        load_pipeline_config(path)


def test_inverted_delay_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """\
platform: instagram
paths:
  data_dir: /data/zeezazum
collect:
  delay_between_profiles_seconds: [40, 15]
transfer:
  bucket: meu-bucket
  prefix: campanha/
""",
    )
    with pytest.raises(ValidationError):
        load_pipeline_config(path)


@pytest.mark.parametrize(
    ("where", "value"),
    [
        (("collect", "max_posts"), 0),
        (("collect", "delay_between_profiles_seconds"), [-1, 40]),
        (("collect", "delay_between_profiles_seconds"), [15, -1]),
        (("collect", "profile_timeout_seconds"), 0),
        (("collect", "retry", "max_attempts"), 0),
        (("collect", "retry", "backoff_seconds"), -1),
        (("download", "max_attempts"), 0),
        (("download", "max_concurrent"), 0),
        (("download", "request_timeout_seconds"), 0),
        (("download", "poll_interval_seconds"), 0),
        (("transfer", "poll_interval_seconds"), 0),
        (("transfer", "ledger_backup_interval_seconds"), 0),
        (("reconcile", "pending_min_age_hours"), -1),
    ],
)
def test_numeric_lower_bounds_are_rejected(
    tmp_path: Path, where: tuple[str, ...], value: object
) -> None:
    raw: dict[str, Any] = {
        "platform": "instagram",
        "paths": {"data_dir": "/data/zeezazum"},
        "transfer": {"bucket": "meu-bucket", "prefix": "campanha/"},
    }
    cursor: dict[str, Any] = raw
    for key in where[:-1]:
        nested = cursor.setdefault(key, {})
        assert isinstance(nested, dict)
        cursor = nested
    cursor[where[-1]] = value
    path = _write(tmp_path, yaml.dump(raw, allow_unicode=True))
    with pytest.raises(ValidationError) as caught:
        load_pipeline_config(path)
    assert any(error["type"] == "greater_than_equal" for error in caught.value.errors())


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("campanha2026-v2", "campanha2026-v2/"),
        ("/campanha2026-v2/", "campanha2026-v2/"),
        ("/campanha2026-v2", "campanha2026-v2/"),
        ("campanha2026-v2/", "campanha2026-v2/"),
    ],
)
def test_prefix_is_normalized(tmp_path: Path, prefix: str, expected: str) -> None:
    path = _write(
        tmp_path,
        f"""\
platform: instagram
paths:
  data_dir: /data/zeezazum
transfer:
  bucket: meu-bucket
  prefix: {prefix}
""",
    )
    cfg = load_pipeline_config(path)
    assert cfg.transfer.prefix == expected
