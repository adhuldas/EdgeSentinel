from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from edgesentinel import EdgeSentinel, RuntimeState
from edgesentinel.core.events import Event, StateChangeEvent
from edgesentinel.core.exceptions import RuntimeAlreadyStartedError, RuntimeNotStartedError
from edgesentinel.durability.journal import IntentStatus
from edgesentinel.metrics.checks import HardwareStatus
from edgesentinel.network.monitor import NetworkLayer
from edgesentinel.persistence.database import Database


class FlakyError(Exception):
    pass


async def test_start_reaches_healthy_and_creates_data_dir_and_db(tmp_path: Path) -> None:
    data_dir = tmp_path / "edgesentinel-data"
    guard = EdgeSentinel("gateway-01", data_dir=data_dir)

    await guard.start()
    try:
        assert guard.state is RuntimeState.HEALTHY
        assert data_dir.is_dir()
        assert (data_dir / "gateway-01.sqlite3").exists()
    finally:
        await guard.stop()


async def test_stop_reaches_stopped(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    await guard.stop()
    assert guard.state is RuntimeState.STOPPED


async def test_double_start_raises(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    try:
        with pytest.raises(RuntimeAlreadyStartedError):
            await guard.start()
    finally:
        await guard.stop()


async def test_stop_before_start_raises(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    with pytest.raises(RuntimeNotStartedError):
        await guard.stop()


async def test_double_stop_is_idempotent(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    await guard.stop()
    await guard.stop()  # must not raise
    assert guard.state is RuntimeState.STOPPED


async def test_empty_name_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        EdgeSentinel("", data_dir=tmp_path)


async def test_on_state_change_decorator_receives_ordered_transitions(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    seen: list[StateChangeEvent] = []

    @guard.on_state_change
    async def handle(event: StateChangeEvent) -> None:
        seen.append(event)

    await guard.start()
    await guard.stop()

    transitions = [(e.previous, e.current) for e in seen]
    assert transitions == [
        (RuntimeState.BOOTING, RuntimeState.INITIALIZING),
        (RuntimeState.INITIALIZING, RuntimeState.HEALTHY),
        (RuntimeState.HEALTHY, RuntimeState.STOPPING),
        (RuntimeState.STOPPING, RuntimeState.STOPPED),
    ]


async def test_runtime_state_is_persisted_across_start_and_stop(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    row = await guard.database.load_runtime_state()
    assert row is not None
    assert row["name"] == "gateway-01"
    assert row["state"] == RuntimeState.HEALTHY.value

    await guard.stop()

    # stop() closes the connection, mirroring a real process exit -- read
    # back through a fresh connection to the same file, as a restart would.
    reopened = Database(tmp_path / "gateway-01.sqlite3")
    await reopened.connect()
    row = await reopened.load_runtime_state()
    assert row is not None
    assert row["state"] == RuntimeState.STOPPED.value
    await reopened.close()


async def test_async_context_manager_starts_and_stops(tmp_path: Path) -> None:
    async with EdgeSentinel("gateway-01", data_dir=tmp_path) as guard:
        state_while_open = guard.state
        assert state_while_open is RuntimeState.HEALTHY
    state_after_close = guard.state
    assert state_after_close is RuntimeState.STOPPED


async def test_failed_initialization_leaves_runtime_in_failed_state(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)

    async def broken_init() -> None:
        raise RuntimeError("simulated init failure")

    guard._on_init = broken_init  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="simulated init failure"):
        await guard.start()

    assert guard.state is RuntimeState.FAILED
    assert not guard.database.is_connected


async def test_concurrent_start_calls_only_one_succeeds(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)

    results = await asyncio.gather(
        guard.start(), guard.start(), guard.start(), return_exceptions=True
    )
    successes = [r for r in results if r is None]
    failures = [r for r in results if isinstance(r, RuntimeAlreadyStartedError)]

    assert len(successes) == 1
    assert len(failures) == 2
    assert guard.state is RuntimeState.HEALTHY

    await guard.stop()


async def test_reliable_decorator_retries_and_publishes_events_on_the_runtime_bus(
    tmp_path: Path,
) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    guard.events.subscribe(collect)

    calls = 0

    @guard.reliable(retries=3, initial_delay=0.0, jitter=False)
    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise FlakyError("first attempt fails")
        return "ok"

    assert await flaky() == "ok"
    assert calls == 2
    assert [e.type for e in seen] == ["retry_attempt", "operation_succeeded"]
    assert all(e.component == "gateway-01" for e in seen)


async def test_reliable_decorator_usable_before_start(tmp_path: Path) -> None:
    # Decorators typically run at import time, before start() is ever
    # called -- reliable() must not require the runtime to be running.
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)

    @guard.reliable()
    async def op() -> str:
        return "ok"

    assert await op() == "ok"


async def test_durable_decorator_journals_a_successful_call(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    try:
        recorded: list[str] = []

        @guard.durable("send_reading")
        async def send_reading(sensor_id: str, value: float) -> None:
            recorded.append(sensor_id)

        await send_reading(sensor_id="temp-1", value=21.5)

        # A completed intent no longer shows up in pending(); read the raw
        # row back through the database directly to check what was journaled.
        row = await guard.database.fetchone(
            "SELECT * FROM intents WHERE operation = 'send_reading'"
        )
        assert row is not None
        assert row["status"] == IntentStatus.COMPLETED.value
        assert row["payload"] == '{"sensor_id": "temp-1", "value": 21.5}'
        assert recorded == ["temp-1"]
    finally:
        await guard.stop()


async def test_durable_decorator_registration_works_before_start(tmp_path: Path) -> None:
    # Decoration (registration) runs at import time same as reliable(), and
    # must not require the runtime to already be started.
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)

    @guard.durable("op")
    async def op() -> str:
        return "ok"

    assert callable(op)


async def test_durable_decorator_call_before_start_raises(tmp_path: Path) -> None:
    # Unlike reliable(), calling a durable operation writes to the journal
    # before it can run at all -- it needs the database connection start()
    # opens, so it can't work before start() the way reliable() can.
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)

    @guard.durable("op")
    async def op() -> str:
        return "ok"

    with pytest.raises(RuntimeNotStartedError):
        await op()


async def test_durable_operation_replays_after_a_restart(tmp_path: Path) -> None:
    # First "boot": the operation fails once and is left pending in the
    # journal -- simulating a crash or reboot before it could succeed.
    first_boot = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await first_boot.start()

    @first_boot.durable("publish")
    async def failing_publish(value: int) -> None:
        raise FlakyError("downstream unavailable")

    with pytest.raises(FlakyError):
        await failing_publish(value=7)
    await first_boot.stop()

    # Second "boot": a fresh EdgeSentinel instance over the same data directory
    # and name, standing in for the process restarting. Registering the
    # same operation name lets start() replay the pending intent.
    second_boot = EdgeSentinel("gateway-01", data_dir=tmp_path)
    calls: list[int] = []

    @second_boot.durable("publish")
    async def working_publish(value: int) -> None:
        calls.append(value)

    await second_boot.start()
    try:
        assert calls == [7]
        assert await second_boot.journal.pending() == []
    finally:
        await second_boot.stop()


async def test_recovery_false_disables_replay_on_start(tmp_path: Path) -> None:
    first_boot = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await first_boot.start()

    @first_boot.durable("publish")
    async def failing_publish() -> None:
        raise FlakyError("downstream unavailable")

    with pytest.raises(FlakyError):
        await failing_publish()
    await first_boot.stop()

    second_boot = EdgeSentinel("gateway-01", data_dir=tmp_path, recovery=False)
    calls = 0

    @second_boot.durable("publish")
    async def working_publish() -> None:
        nonlocal calls
        calls += 1

    await second_boot.start()
    try:
        assert calls == 0
        pending = await second_boot.journal.pending()
        assert len(pending) == 1
    finally:
        await second_boot.stop()


async def test_durable_decorator_publishes_events_on_the_runtime_bus(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    guard.events.subscribe(collect)
    await guard.start()
    try:

        @guard.durable("op")
        async def op() -> None:
            pass

        await op()

        durable_events = [e for e in seen if e.type.startswith("durable_operation_")]
        assert [e.type for e in durable_events] == [
            "durable_operation_started",
            "durable_operation_completed",
        ]
        assert all(e.component == "gateway-01" for e in durable_events)
    finally:
        await guard.stop()


async def test_timeline_and_incidents_are_accessible_before_start(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)

    assert guard.incidents.open_incident is None
    assert guard.incidents.incidents == ()
    # Querying the timeline before start() requires a connected database,
    # same as any other Database method -- see RuntimeNotStartedError-style
    # guards elsewhere. Just confirm the property itself doesn't raise.
    assert guard.timeline is not None


async def test_boot_transitions_are_recorded_on_the_timeline(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    try:
        events = await guard.timeline.query(type="state_change", newest_first=False)
        transitions = [(e.metadata["previous"], e.metadata["current"]) for e in events]
        assert transitions == [
            ("booting", "initializing"),
            ("initializing", "healthy"),
        ]
    finally:
        await guard.stop()


async def test_stop_transition_is_recorded_before_detaching(tmp_path: Path) -> None:
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    await guard.stop()

    # The runtime's own DB handle is closed post-stop, so re-open a fresh
    # one pointed at the same file -- exactly what a separate CLI process
    # inspecting a stopped runtime would do.
    db = Database(tmp_path / "gateway-01.sqlite3")
    await db.connect()
    try:
        rows = await db.load_events(type="state_change", newest_first=False)
        # booting->initializing, initializing->healthy, healthy->stopping, stopping->stopped
        assert len(rows) == 4
        assert '"current": "stopped"' in rows[-1]["metadata"]
    finally:
        await db.close()


async def test_events_published_after_stop_are_not_persisted(tmp_path: Path) -> None:
    """Detach must happen before the database is closed, and must actually
    stop the EventLog handler -- otherwise a late publish would try to
    write to a closed connection and raise."""
    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await guard.start()
    await guard.stop()

    # Must not raise even though the database is now closed.
    await guard.events.publish(Event(type="late", component="test"))


async def test_incidents_are_tracked_end_to_end_via_a_registered_network_monitor(
    tmp_path: Path,
) -> None:
    link_up = True

    async def link_check() -> bool:
        return link_up

    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    monitor = guard.watch_network({NetworkLayer.LINK: link_check})

    await guard.start()
    try:
        assert guard.state is RuntimeState.HEALTHY
        assert guard.incidents.open_incident is None

        link_up = False
        await monitor.check_once()

        # mypy narrows `guard.state` to HEALTHY at the assert above and
        # doesn't invalidate that narrowing across the `await` that changes
        # it -- same known false positive as elsewhere in this suite.
        assert guard.state is RuntimeState.OFFLINE  # type: ignore[comparison-overlap]
        open_incident = guard.incidents.open_incident
        assert open_incident is not None
        assert RuntimeState.OFFLINE in open_incident.states

        link_up = True
        await monitor.check_once()
        await monitor.check_once()  # OFFLINE -> HEALTHY passes through DEGRADED first

        assert guard.state is RuntimeState.HEALTHY
        assert guard.incidents.open_incident is None
        assert len(guard.incidents.incidents) == 1

        recorded = await guard.timeline.query(type="state_change", newest_first=False)
        assert any(e.metadata["current"] == "offline" for e in recorded)
    finally:
        await guard.stop()


async def test_watch_hardware_drives_the_runtime_to_degraded_and_back(
    tmp_path: Path,
) -> None:
    cpu_load = 0.1

    def metrics_check() -> HardwareStatus:
        return HardwareStatus(cpu_load_ratio=cpu_load, memory_used_ratio=0.1)

    guard = EdgeSentinel("gateway-01", data_dir=tmp_path)
    monitor = guard.watch_hardware(cpu_high=0.5, metrics_check=metrics_check)

    await guard.start()
    try:
        assert guard.state is RuntimeState.HEALTHY

        cpu_load = 0.95
        await monitor.check_once()

        assert guard.state is RuntimeState.DEGRADED  # type: ignore[comparison-overlap]
        open_incident = guard.incidents.open_incident
        assert open_incident is not None

        cpu_load = 0.1
        await monitor.check_once()

        assert guard.state is RuntimeState.HEALTHY
        assert guard.incidents.open_incident is None

        recorded = await guard.timeline.query(type="hardware_metrics_high", newest_first=False)
        assert len(recorded) == 1
        assert recorded[0].component == guard.name
    finally:
        await guard.stop()


async def test_a_fresh_runtime_re_attaches_timeline_and_incidents_cleanly(
    tmp_path: Path,
) -> None:
    """A stopped EdgeSentinel is terminal and can't be restarted in place, but
    a fresh instance pointed at the same data directory (e.g. after a
    process restart) must attach and record events just as cleanly."""
    first = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await first.start()
    await first.stop()

    second = EdgeSentinel("gateway-01", data_dir=tmp_path)
    await second.start()
    try:
        # first's own boot + shutdown sequence (4 events) is already in the
        # same on-disk database -- only the 2 newest belong to `second`.
        newest = await second.timeline.query(type="state_change", limit=2)
        assert [(e.metadata["previous"], e.metadata["current"]) for e in reversed(newest)] == [
            ("booting", "initializing"),
            ("initializing", "healthy"),
        ]
    finally:
        await second.stop()
