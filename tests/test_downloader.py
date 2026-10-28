"""Baixador: HTTP falso, sem rede."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from zeezazum.config import (
    DownloadConfig,
    PipelineConfig,
    PipelinePathsConfig,
    TransferConfig,
)
from zeezazum.ledger import append, read_all
from zeezazum.locks import single_instance
from zeezazum.paths import DataPaths
from zeezazum.pipeline import downloader
from zeezazum.pipeline.downloader import _drain_once, run_download

ROOT = Path(__file__).resolve().parents[1]
_FIVE_MIB = 5 * 1024 * 1024


class _ScriptedTransport(httpx.AsyncBaseTransport):
    def __init__(self, outcomes: list[httpx.Response | BaseException]) -> None:
        self._outcomes = list(outcomes)
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if not self._outcomes:
            raise AssertionError(f"requisição inesperada: {request.url}")
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def aclose(self) -> None:
        return None


class _GuardedResponse(httpx.Response):
    @property
    def content(self) -> bytes:  # type: ignore[override]
        raise AssertionError("response.content não deve ser lido")


def _config(data_dir: Path, **download: Any) -> PipelineConfig:
    return PipelineConfig(
        paths=PipelinePathsConfig(data_dir=str(data_dir)),
        download=DownloadConfig(**download),
        transfer=TransferConfig(bucket="meu-bucket", prefix="campanha/"),
    )


def _paths(tmp_path: Path) -> DataPaths:
    paths = DataPaths(tmp_path / "data")
    paths.ensure()
    return paths


def _todo(
    handle: str,
    shortcode: str,
    index: int,
    *,
    kind: str = "image",
    url: str = "https://cdn.example/a.jpg",
    stamp: str = "20261028T130501Z",
) -> dict[str, Any]:
    item_id = f"{shortcode}_{index}"
    return {
        "todo_id": f"{item_id}@{stamp}",
        "item_id": item_id,
        "collection_id": f"{handle}@{stamp}",
        "run_id": "lote",
        "handle": handle,
        "shortcode": shortcode,
        "index": index,
        "kind": kind,
        "media_url": url,
        "post_url": f"https://www.instagram.com/p/{shortcode}/",
        "dest_stem": f"instagram/{handle}/media/{item_id}",
        "collected_at": "2026-10-28T13:05:01+00:00",
    }


def _jpeg(body: bytes = b"jpeg") -> httpx.Response:
    return httpx.Response(200, content=body, headers={"content-type": "image/jpeg"})


def _png(body: bytes = b"\x89PNG") -> httpx.Response:
    return httpx.Response(200, content=body, headers={"content-type": "image/png"})


def _parts(paths: DataPaths) -> list[Path]:
    return list(paths.output.rglob("*.part"))


def test_download_lock_returns_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    with single_instance(paths.lock_file("download"), "download"):
        code = run_download(cfg, paths, max_cycles=1)
    assert code == 2
    assert "outra instância de download está em execução" in capsys.readouterr().err


def test_png_success_writes_final_file(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    record = _todo("fulano", "aaa", 0, url="https://cdn.example/aaa.png")
    append(paths.todo_file("fulano"), record)
    body = b"\x89PNG-dados"
    code = run_download(cfg, paths, transport=_ScriptedTransport([_png(body)]), max_cycles=1)
    assert code == 0
    final = paths.output / "instagram/fulano/media/aaa_0.png"
    assert final.read_bytes() == body
    assert _parts(paths) == []
    done = read_all(paths.done_file("fulano"))
    assert len(done) == 1
    assert done[0]["status"] == "ok"
    assert done[0]["http_status"] == 200
    assert done[0]["error"] is None
    assert done[0]["file"] == "instagram/fulano/media/aaa_0.png"
    assert done[0]["bytes"] == len(body)
    assert done[0]["attempts"] == 1
    assert str(done[0]["finished_at"]).endswith("+00:00")


async def test_403_and_404_are_not_retried(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir, max_attempts=3)
    denied = _todo("fulano", "nope", 0, url="https://cdn.example/nope")
    missing = _todo("fulano", "gone", 0, url="https://cdn.example/gone")
    append(paths.todo_file("fulano"), denied)
    append(paths.todo_file("fulano"), missing)
    transport = _ScriptedTransport([httpx.Response(403), httpx.Response(404)])
    await _drain_once(cfg, paths, {}, transport=transport)
    assert transport.calls == 2
    done = read_all(paths.done_file("fulano"))
    by_item = {row["item_id"]: row for row in done}
    assert by_item["nope_0"]["http_status"] == 403
    assert by_item["gone_0"]["http_status"] == 404
    for row in done:
        assert row["status"] == "failed"
        assert row["attempts"] == 1
        assert row["file"] is None
        assert row["bytes"] is None
        assert row["error"]
    assert _parts(paths) == []


async def test_503_then_200_counts_attempts(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir, max_attempts=3)
    record = _todo("fulano", "aaa", 0)
    append(paths.todo_file("fulano"), record)
    body = b"ok"
    transport = _ScriptedTransport([httpx.Response(503), httpx.Response(503), _jpeg(body)])
    await _drain_once(cfg, paths, {}, transport=transport)
    assert transport.calls == 3
    done = read_all(paths.done_file("fulano"))
    assert len(done) == 1
    assert done[0]["status"] == "ok"
    assert done[0]["http_status"] == 200
    assert done[0]["attempts"] == 3
    assert done[0]["bytes"] == len(body)
    assert _parts(paths) == []


async def test_timeout_fails_without_http_status(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir, max_attempts=3)
    record = _todo("fulano", "aaa", 0)
    append(paths.todo_file("fulano"), record)
    transport = _ScriptedTransport([httpx.ReadTimeout("tempo esgotado")] * 3)
    await _drain_once(cfg, paths, {}, transport=transport)
    assert transport.calls == 3
    done = read_all(paths.done_file("fulano"))
    assert len(done) == 1
    assert done[0]["status"] == "failed"
    assert done[0]["http_status"] is None
    assert done[0]["attempts"] == 3
    assert done[0]["file"] is None
    assert done[0]["bytes"] is None
    assert "Timeout" in str(done[0]["error"])
    assert _parts(paths) == []


async def test_idempotency_skips_answered_and_reuses_file(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    answered = _todo("fulano", "old", 0, url="https://cdn.example/old")
    duplicate = _todo(
        "fulano",
        "dup",
        0,
        url="https://cdn.example/dup",
        stamp="20261028T140000Z",
    )
    on_disk = _todo("fulano", "disk", 0, url="https://cdn.example/disk")
    append(
        paths.done_file("fulano"),
        {
            "todo_id": answered["todo_id"],
            "item_id": answered["item_id"],
            "status": "failed",
            "http_status": 404,
            "error": "DownloadHTTPError: HTTP 404",
            "file": None,
            "bytes": None,
            "attempts": 1,
            "finished_at": "2026-10-28T13:06:00+00:00",
        },
    )
    append(
        paths.done_file("fulano"),
        {
            "todo_id": "dup_0@20261028T100000Z",
            "item_id": duplicate["item_id"],
            "status": "ok",
            "http_status": 200,
            "error": None,
            "file": "instagram/fulano/media/dup_0.jpg",
            "bytes": 4,
            "attempts": 1,
            "finished_at": "2026-10-28T10:00:00+00:00",
        },
    )
    final = paths.output / "instagram/fulano/media/disk_0.jpg"
    final.parent.mkdir(parents=True, exist_ok=True)
    payload = b"ja-existe"
    final.write_bytes(payload)
    for record in (answered, duplicate, on_disk):
        append(paths.todo_file("fulano"), record)
    transport = _ScriptedTransport([])
    await _drain_once(cfg, paths, {}, transport=transport)
    assert transport.calls == 0
    done = read_all(paths.done_file("fulano"))
    assert [row["todo_id"] for row in done] == [
        answered["todo_id"],
        "dup_0@20261028T100000Z",
        on_disk["todo_id"],
    ]
    recorded = done[-1]
    assert recorded["status"] == "ok"
    assert recorded["attempts"] == 0
    assert recorded["bytes"] == len(payload)
    assert recorded["file"] == "instagram/fulano/media/disk_0.jpg"
    assert recorded["http_status"] is None


async def test_later_lines_wait_until_complete(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    first = _todo("fulano", "aaa", 0, url="https://cdn.example/aaa")
    second = _todo("fulano", "bbb", 0, url="https://cdn.example/bbb")
    third = _todo("fulano", "ccc", 0, url="https://cdn.example/ccc")
    raw = (json.dumps(first) + "\n" + json.dumps(second)).encode()
    todo = paths.todo_file("fulano")
    todo.write_bytes(raw[:-8])
    transport = _ScriptedTransport([_jpeg(b"a"), _jpeg(b"b"), _jpeg(b"c")])
    offsets: dict[str, int] = {}
    await _drain_once(cfg, paths, offsets, transport=transport)
    assert [row["item_id"] for row in read_all(paths.done_file("fulano"))] == ["aaa_0"]
    assert not (paths.output / "instagram/fulano/media/bbb_0.jpg").exists()
    with todo.open("ab") as handle:
        handle.write(raw[-8:])
        handle.write(b"\n")
        handle.write((json.dumps(third) + "\n").encode())
    await _drain_once(cfg, paths, offsets, transport=transport)
    assert sorted(row["item_id"] for row in read_all(paths.done_file("fulano"))) == [
        "aaa_0",
        "bbb_0",
        "ccc_0",
    ]
    assert transport.calls == 3


async def test_large_body_is_written_in_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    record = _todo("fulano", "big", 0, url="https://cdn.example/big")
    append(paths.todo_file("fulano"), record)
    body = b"x" * _FIVE_MIB
    sizes: list[int] = []
    original = downloader._append_block

    def _spy(path: Path, data: bytes) -> None:
        sizes.append(len(data))
        original(path, data)

    monkeypatch.setattr(downloader, "_append_block", _spy)
    response = _GuardedResponse(
        200,
        stream=httpx.ByteStream(body),
        headers={"content-type": "image/jpeg"},
    )
    await _drain_once(cfg, paths, {}, transport=_ScriptedTransport([response]))
    assert sizes == [downloader._BLOCK_BYTES] * 5
    final = paths.output / "instagram/fulano/media/big_0.jpg"
    assert final.stat().st_size == _FIVE_MIB
    assert _parts(paths) == []


async def test_video_keeps_mp4_even_with_png_content_type(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    record = _todo("fulano", "vid", 0, kind="video", url="https://cdn.example/v.mp4")
    append(paths.todo_file("fulano"), record)
    body = b"mp4"
    response = httpx.Response(200, content=body, headers={"content-type": "image/png"})
    await _drain_once(cfg, paths, {}, transport=_ScriptedTransport([response]))
    final = paths.output / "instagram/fulano/media/vid_0.mp4"
    assert final.read_bytes() == body
    done = read_all(paths.done_file("fulano"))
    assert done[0]["file"] == "instagram/fulano/media/vid_0.mp4"
    assert done[0]["bytes"] == len(body)


async def test_disabled_videos_are_skipped(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir, download_videos=False)
    video = _todo("fulano", "vid", 0, kind="video", url="https://cdn.example/v.mp4")
    image = _todo("fulano", "img", 0, kind="image", url="https://cdn.example/i.jpg")
    append(paths.todo_file("fulano"), video)
    append(paths.todo_file("fulano"), image)
    transport = _ScriptedTransport([_jpeg(b"img")])
    await _drain_once(cfg, paths, {}, transport=transport)
    assert transport.calls == 1
    done = read_all(paths.done_file("fulano"))
    assert [row["item_id"] for row in done] == ["img_0"]
    assert not (paths.output / "instagram/fulano/media/vid_0.mp4").exists()


class _HoldTransport(httpx.AsyncBaseTransport):
    """Segura cada resposta até ``release``; avisa quando a segunda começa."""

    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        self._started = started
        self._release = release
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.calls == 2:
            self._started.set()
        await self._release.wait()
        return httpx.Response(200, content=b"x", headers={"content-type": "image/jpeg"})

    async def aclose(self) -> None:
        return None


async def test_stop_records_only_downloads_already_started(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir, max_concurrent=2)
    for index in range(10):
        append(
            paths.todo_file("fulano"),
            _todo("fulano", f"p{index}", 0, url=f"https://cdn.example/{index}.jpg"),
        )
    started = asyncio.Event()
    release = asyncio.Event()
    stop = asyncio.Event()
    transport = _HoldTransport(started, release)

    async def _release_after_two() -> None:
        await started.wait()
        stop.set()
        release.set()

    trigger = asyncio.create_task(_release_after_two())
    await asyncio.wait_for(
        _drain_once(cfg, paths, {}, transport=transport, stop=stop),
        timeout=5,
    )
    await trigger
    assert len(read_all(paths.done_file("fulano"))) == 2
    assert transport.calls == 2
    assert _parts(paths) == []


async def test_invalid_url_fails_item_and_continues(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    invalid = _todo("fulano", "bad", 0, url="ht!tp://x")
    valid = _todo("fulano", "ok", 0, url="https://cdn.example/ok.jpg")
    append(paths.todo_file("fulano"), invalid)
    append(paths.todo_file("fulano"), valid)
    await _drain_once(
        cfg,
        paths,
        {},
        transport=_ScriptedTransport([_jpeg(b"ok"), _jpeg(b"ok")]),
    )
    done = {row["item_id"]: row for row in read_all(paths.done_file("fulano"))}
    assert done["bad_0"]["status"] == "failed"
    assert done["bad_0"]["http_status"] is None
    assert done["ok_0"]["status"] == "ok"
    assert (paths.output / "instagram/fulano/media/ok_0.jpg").read_bytes() == b"ok"


async def test_oserror_propagates_without_done_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths(tmp_path)
    cfg = _config(paths.data_dir)
    record = _todo("fulano", "aaa", 0)
    append(paths.todo_file("fulano"), record)

    def _boom(path: Path, data: bytes) -> None:
        raise OSError("disco cheio")

    monkeypatch.setattr(downloader, "_append_block", _boom)
    with pytest.raises(OSError, match="disco cheio"):
        await _drain_once(cfg, paths, {}, transport=_ScriptedTransport([_jpeg(b"abc")]))
    assert read_all(paths.done_file("fulano")) == []


def test_downloader_does_not_import_browser_or_s3() -> None:
    source = (ROOT / "src" / "zeezazum" / "pipeline" / "downloader.py").read_text(encoding="utf-8")
    assert "playwright" not in source
    assert "boto3" not in source
    assert "response.content" not in source
