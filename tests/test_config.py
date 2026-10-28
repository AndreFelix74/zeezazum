"""Configuração do pipeline, sem alterar o config legado."""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from zeezazum.config import load_config, load_pipeline_config

ROOT = Path(__file__).resolve().parents[1]


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_example_is_valid() -> None:
    cfg = load_pipeline_config(ROOT / "config.pipeline.example.yaml")
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


def test_legacy_config_still_loads() -> None:
    cfg = load_config(ROOT / "config.example.yaml")
    assert cfg.platform == "instagram"
    assert cfg.input.url_column == "instagram_url"


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
