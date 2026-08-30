from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from edgeguard import EdgeGuard
from edgeguard.cli.main import main
from edgeguard.network.monitor import NetworkLayer


def _seed_runtime(data_dir: Path, name: str = "gateway-01") -> None:
    """Start and stop a real EdgeGuard runtime against ``data_dir``, so the
    CLI has an actual on-disk database -- created the same way a real
    device would -- to read."""

    async def run() -> None:
        guard = EdgeGuard(name, data_dir=data_dir)
        await guard.start()
        await guard.stop()

    asyncio.run(run())


def _seed_runtime_with_incident(data_dir: Path, name: str = "gateway-01") -> None:
    link_up = True

    async def link_check() -> bool:
        return link_up

    async def run() -> None:
        nonlocal link_up
        guard = EdgeGuard(name, data_dir=data_dir)
        monitor = guard.watch_network({NetworkLayer.LINK: link_check})
        await guard.start()
        link_up = False
        await monitor.check_once()
        link_up = True
        await monitor.check_once()
        await monitor.check_once()
        await guard.stop()

    asyncio.run(run())


def test_missing_database_reports_an_error_and_does_not_create_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code = main(["--name", "no-such-gateway", "--data-dir", str(tmp_path), "status"])

    assert exit_code == 1
    assert "no database found" in capsys.readouterr().err
    assert not (tmp_path / "no-such-gateway.sqlite3").exists()


def test_status_reports_last_persisted_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_runtime(tmp_path)

    exit_code = main(["--name", "gateway-01", "--data-dir", str(tmp_path), "status"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "gateway-01" in out
    assert "stopped" in out


def test_timeline_lists_recorded_events(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed_runtime(tmp_path)

    exit_code = main(["--name", "gateway-01", "--data-dir", str(tmp_path), "timeline"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "state_change" in out
    assert "runtime" in out


def test_timeline_filters_by_type(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed_runtime(tmp_path)

    exit_code = main(
        [
            "--name",
            "gateway-01",
            "--data-dir",
            str(tmp_path),
            "timeline",
            "--type",
            "no_such_event_type",
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out.strip() == "(no events)"


def test_timeline_oldest_first_shows_boot_sequence_in_order(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_runtime(tmp_path)

    exit_code = main(
        [
            "--name",
            "gateway-01",
            "--data-dir",
            str(tmp_path),
            "timeline",
            "--type",
            "state_change",
            "--oldest-first",
        ]
    )

    assert exit_code == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 4  # booting->init, init->healthy, healthy->stopping, stopping->stopped
    assert "state_change" in lines[0]


def test_timeline_respects_limit(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed_runtime(tmp_path)

    exit_code = main(
        ["--name", "gateway-01", "--data-dir", str(tmp_path), "timeline", "--limit", "1"]
    )

    assert exit_code == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1


def test_incidents_reports_a_resolved_incident(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_runtime_with_incident(tmp_path)

    exit_code = main(["--name", "gateway-01", "--data-dir", str(tmp_path), "incidents"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "resolved" in out
    assert "1 resolved incident(s)" in out


def test_incidents_with_no_incidents(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed_runtime(tmp_path)

    exit_code = main(["--name", "gateway-01", "--data-dir", str(tmp_path), "incidents"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "(no incidents)" in out
    assert "0 resolved incident(s)" in out


def test_missing_required_arguments_exits_nonzero(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code != 0
