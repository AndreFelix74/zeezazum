"""Transferidor: destino em memória, sem rede."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from zeezazum.config import PipelineConfig, PipelinePathsConfig, TransferConfig
from zeezazum.ledger import append, read_all
from zeezazum.locks import single_instance
from zeezazum.paths import DataPaths
from zeezazum.pipeline import transfer as transfer_mod
from zeezazum.pipeline.transfer import (
    _Stop,
    _backup_ledger,
    _drain_once,
    _next_backoff,
    _sweep_metadata,
    run_transfer,
)
from zeezazum.remote import S3ObjectStore

ROOT = Path(__file__).resolve().parents[1]


class _MemoryStore:
    def __init__(self, *, fail_uploads: int = 0, head_delta: int = 0) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: list[str] = []
        self.fail_uploads = fail_uploads
        self.head_delta = head_delta

    def head_size(self, key: str) -> int | None:
        if key not in self.objects:
            return None
        return len(self.objects[key]) + self.head_delta

    def upload_file(self, local_path: Path, key: str) -> None:
        if self.fail_uploads:
            self.fail_uploads -= 1
            raise RuntimeError("falha no destino")
        self.objects[key] = local_path.read_bytes()
        self.uploads.append(key)


def _config(data_dir: Path, **transfer: Any) -> PipelineConfig:
    return PipelineConfig(
        paths=PipelinePathsConfig(data_dir=str(data_dir)),
        transfer=TransferConfig(bucket="meu-bucket", prefix="campanha/", **transfer),
    )


def _paths(tmp_path: Path) -> DataPaths:
    paths = DataPaths(tmp_path / "data")
    paths.ensure()
    return paths


def _write(paths: DataPaths, relative: str, body: bytes) -> Path:
    target = paths.output / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    return target


def _ok(item_id: str, file_rel: str, nbytes: int) -> dict[str, Any]:
    return {
        "todo_id": f"{item_id}@20261028T130501Z",
        "item_id": item_id,
        "status": "ok",
        "http_status": 200,
        "error": None,
        "file": file_rel,
        "bytes": nbytes,
        "attempts": 1,
        "finished_at": "2026-10-28T13:06:10+00:00",
    }


def _failed(item_id: str) -> dict[str, Any]:
    return {
        "todo_id": f"{item_id}@20261028T130501Z",
        "item_id": item_id,
        "status": "failed",
        "http_status": 404,
        "error": "DownloadHTTPError: HTTP 404",
        "file": None,
        "bytes": None,
        "attempts": 1,
        "finished_at": "2026-10-28T13:06:10+00:00",
    }


def test_transfer_lock_returns_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    with single_instance(paths.lock_file("transfer"), "transfer"):
        code = run_transfer(cfg, paths, store=_MemoryStore(), max_cycles=1)
    assert code == 2
    assert "outra instância de transfer está em execução" in capsys.readouterr().err


def test_media_is_moved_and_metadata_stays(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    media = "instagram/fulano/media/aaa_0.jpg"
    body = b"jpeg-bytes"
    media_path = _write(paths, media, body)
    posts = _write(paths, "instagram/fulano/posts.json", b'{"posts":[]}')
    raw = _write(paths, "instagram/fulano/raw/20261028.jsonl.gz", b"raw")
    parquet = _write(paths, "instagram/fulano/posts.parquet", b"parquet")
    profile = _write(paths, "instagram/fulano/profile.json", b"{}")
    append(paths.done_file("fulano"), _ok("aaa_0", media, len(body)))
    store = _MemoryStore()
    progressed, failed = _drain_once(cfg, paths, {}, store)
    assert progressed is True
    assert failed is False
    assert not media_path.exists()
    assert posts.is_file() and raw.is_file() and parquet.is_file() and profile.is_file()
    uploaded = read_all(paths.uploaded_file("fulano"))
    kinds = [row["kind"] for row in uploaded]
    assert kinds[0] == "media"
    media_row = uploaded[0]
    assert media_row["item_id"] == "aaa_0"
    assert media_row["file"] == media
    assert media_row["key"] == f"campanha/{media}"
    assert media_row["bytes"] == len(body)
    assert media_row["local_mtime"] is None
    assert store.objects[f"campanha/{media}"] == body
    assert store.uploads == [f"campanha/{media}"]
    assert all(row["kind"] == "media" for row in uploaded)


def test_divergent_remote_size_keeps_local_file(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    media = "instagram/fulano/media/aaa_0.jpg"
    media_path = _write(paths, media, b"jpeg")
    append(paths.done_file("fulano"), _ok("aaa_0", media, 4))
    store = _MemoryStore(head_delta=5)
    _drain_once(cfg, paths, {}, store)
    assert media_path.read_bytes() == b"jpeg"
    assert read_all(paths.uploaded_file("fulano")) == []


def test_missing_local_file_reuses_remote_object(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    media = "instagram/fulano/media/aaa_0.jpg"
    body = b"jpeg"
    key = f"campanha/{media}"
    store = _MemoryStore()
    store.objects[key] = body
    append(paths.done_file("fulano"), _ok("aaa_0", media, len(body)))
    _drain_once(cfg, paths, {}, store)
    assert store.uploads == []
    uploaded = read_all(paths.uploaded_file("fulano"))
    assert len(uploaded) == 1
    assert uploaded[0]["kind"] == "media"
    assert uploaded[0]["bytes"] == len(body)
    assert uploaded[0]["key"] == key


def test_uploaded_item_is_not_sent_again(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    media = "instagram/fulano/media/aaa_0.jpg"
    media_path = _write(paths, media, b"jpeg")
    append(
        paths.uploaded_file("fulano"),
        {
            "kind": "media",
            "item_id": "aaa_0",
            "file": media,
            "key": f"campanha/{media}",
            "bytes": 4,
            "local_mtime": None,
            "uploaded_at": "2026-10-28T13:07:00+00:00",
        },
    )
    append(paths.done_file("fulano"), _ok("aaa_0", media, 4))
    store = _MemoryStore()
    _drain_once(cfg, paths, {}, store)
    assert store.uploads == []
    assert media_path.is_file()
    assert len(read_all(paths.uploaded_file("fulano"))) == 1


def test_sweep_sends_metadata_without_done_and_tracks_mtime(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    posts = _write(paths, "instagram/fulano/posts.json", b"posts")
    profile = _write(paths, "instagram/fulano/profile.json", b"{}")
    store = _MemoryStore()
    cache: dict[str, dict[str, tuple[int, float]]] = {}
    assert _sweep_metadata(cfg, paths, store, cache, _Stop()) is True
    assert store.uploads == [
        "campanha/instagram/fulano/posts.json",
        "campanha/instagram/fulano/profile.json",
    ]
    rows = read_all(paths.uploaded_file("fulano"))
    assert [row["file"] for row in rows] == [
        "instagram/fulano/posts.json",
        "instagram/fulano/profile.json",
    ]
    assert all(row["kind"] == "meta" and row["item_id"] is None for row in rows)
    store.uploads.clear()
    assert _sweep_metadata(cfg, paths, store, cache, _Stop()) is True
    assert store.uploads == []
    os.utime(profile, (profile.stat().st_atime, profile.stat().st_mtime + 10))
    assert _sweep_metadata(cfg, paths, store, cache, _Stop()) is True
    assert store.uploads == ["campanha/instagram/fulano/profile.json"]
    assert posts.is_file() and profile.is_file()


def test_signal_sends_changed_metadata_before_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir, ledger_backup_interval_seconds=3600)
    profile = _write(paths, "instagram/fulano/profile.json", b"v1")
    store = _MemoryStore()

    def _sleep_and_signal(seconds: float, stop: _Stop) -> bool:
        os.utime(profile, (profile.stat().st_atime, profile.stat().st_mtime + 30))
        stop.set()
        return True

    monkeypatch.setattr(transfer_mod, "_backup_due", lambda last, config: False)
    monkeypatch.setattr(transfer_mod, "_interruptible_sleep", _sleep_and_signal)
    code = run_transfer(cfg, paths, store=store)
    assert code == 0
    assert store.uploads.count("campanha/instagram/fulano/profile.json") == 1
    uploaded = read_all(paths.uploaded_file("fulano"))
    assert [row["file"] for row in uploaded if row["kind"] == "meta"] == [
        "instagram/fulano/profile.json"
    ]


def test_sweep_failure_keeps_backup_schedule_and_backs_off(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths(tmp_path)
    cfg = _config(
        paths.data_dir,
        poll_interval_seconds=7,
        ledger_backup_interval_seconds=3600,
    )
    _write(paths, "instagram/fulano/profile.json", b"{}")
    store = _MemoryStore(fail_uploads=10)
    delays: list[float] = []

    def _record_sleep(seconds: float, stop: _Stop) -> bool:
        delays.append(seconds)
        return len(delays) >= 2

    monkeypatch.setattr(transfer_mod, "_interruptible_sleep", _record_sleep)
    code = run_transfer(cfg, paths, store=store)
    assert code == 0
    assert delays == [30, 60]
    assert store.uploads == []
    assert read_all(paths.uploaded_file("fulano")) == []


def test_remote_failure_retries_on_the_next_cycle(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    media = "instagram/fulano/media/aaa_0.jpg"
    body = b"jpeg"
    media_path = _write(paths, media, body)
    append(paths.done_file("fulano"), _ok("aaa_0", media, len(body)))
    store = _MemoryStore(fail_uploads=1)
    offsets: dict[str, int] = {}
    progressed, failed = _drain_once(cfg, paths, offsets, store)
    assert failed is True
    assert offsets.get("fulano", 0) == 0
    assert read_all(paths.uploaded_file("fulano")) == []
    assert media_path.is_file()
    progressed, failed = _drain_once(cfg, paths, offsets, store)
    assert progressed is True
    assert failed is False
    assert offsets["fulano"] > 0
    assert not media_path.exists()
    uploaded = read_all(paths.uploaded_file("fulano"))
    assert uploaded[0]["item_id"] == "aaa_0"
    assert uploaded[0]["kind"] == "media"


def test_keeps_local_media_when_deletion_is_disabled(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir, delete_local_media_after_upload=False)
    media = "instagram/fulano/media/aaa_0.jpg"
    media_path = _write(paths, media, b"jpeg")
    append(paths.done_file("fulano"), _ok("aaa_0", media, 4))
    store = _MemoryStore()
    _drain_once(cfg, paths, {}, store)
    assert media_path.read_bytes() == b"jpeg"
    uploaded = read_all(paths.uploaded_file("fulano"))
    assert uploaded[0]["kind"] == "media"
    assert uploaded[0]["item_id"] == "aaa_0"
    assert f"campanha/{media}" in store.uploads


def test_failed_done_lines_are_ignored(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    append(paths.done_file("fulano"), _failed("aaa_0"))
    store = _MemoryStore()
    progressed, failed = _drain_once(cfg, paths, {}, store)
    assert progressed is True
    assert failed is False
    assert store.uploads == []
    assert read_all(paths.uploaded_file("fulano")) == []


def test_ledger_backup_sends_each_file_once_until_it_changes(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    append(paths.todo_file("fulano"), {"todo_id": "a"})
    store = _MemoryStore()
    state: dict[str, tuple[int, float]] = {}
    assert _backup_ledger(cfg, paths, store, state, _stop()) is True
    assert store.uploads == ["campanha/_ledger/todo/fulano.jsonl"]
    assert store.objects["campanha/_ledger/todo/fulano.jsonl"]
    store.uploads.clear()
    assert _backup_ledger(cfg, paths, store, state, _stop()) is True
    assert store.uploads == []
    append(paths.todo_file("fulano"), {"todo_id": "b"})
    assert _backup_ledger(cfg, paths, store, state, _stop()) is True
    assert store.uploads == ["campanha/_ledger/todo/fulano.jsonl"]


def test_backoff_doubles_up_to_fifteen_minutes() -> None:
    assert _next_backoff(0) == 30
    assert _next_backoff(30) == 60
    assert _next_backoff(480) == 900
    assert _next_backoff(900) == 900


def test_head_size_maps_missing_object_to_none() -> None:
    class _Client:
        def head_object(self, **kwargs: object) -> dict[str, object]:
            raise ClientError(
                {
                    "Error": {"Code": "404", "Message": "Not Found"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )

    store = S3ObjectStore("meu-bucket", "sa-east-1", client=_Client())
    assert store.head_size("chave") is None


def test_head_size_propagates_other_errors() -> None:
    class _Client:
        def head_object(self, **kwargs: object) -> dict[str, object]:
            raise ClientError(
                {"Error": {"Code": "403", "Message": "Forbidden"}},
                "HeadObject",
            )

    store = S3ObjectStore("meu-bucket", "sa-east-1", client=_Client())
    with pytest.raises(ClientError):
        store.head_size("chave")


def test_upload_file_streams_via_the_client(tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    class _Client:
        def upload_file(self, filename: str, bucket: str, key: str) -> None:
            seen["args"] = (filename, bucket, key)

    local = tmp_path / "arquivo.jpg"
    local.write_bytes(b"abc")
    store = S3ObjectStore("meu-bucket", "sa-east-1", client=_Client())
    store.upload_file(local, "campanha/arquivo.jpg")
    assert seen["args"] == (str(local), "meu-bucket", "campanha/arquivo.jpg")


def test_transfer_modules_stay_isolated() -> None:
    source_root = ROOT / "src" / "zeezazum"
    transfer = (source_root / "pipeline" / "transfer.py").read_text(encoding="utf-8")
    remote = (source_root / "remote.py").read_text(encoding="utf-8")
    assert "playwright" not in transfer and "httpx" not in transfer
    assert "playwright" not in remote and "httpx" not in remote
    assert "boto3" not in transfer
    for path in source_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "delete" + "_object" not in text
        if path.name != "remote.py":
            assert "boto3" not in text


def _stop() -> _Stop:
    return _Stop()
