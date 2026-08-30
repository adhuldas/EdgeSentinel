"""The EdgeGuard runtime: the top-level object applications construct.

``EdgeGuard`` composes the lifecycle state machine, the event bus, and local
SQLite persistence into a single object with a small public surface --
``start()``, ``stop()``, ``on_state_change()``, and read-only state
properties. Later phases attach resilience, network, process-supervision,
and diagnostics subsystems to this same object without growing it into a god
object: each subsystem lives in its own module and this class only wires
them together.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import ParamSpec, Protocol, TypeVar

from edgeguard.core.events import EventBus
from edgeguard.core.exceptions import RuntimeAlreadyStartedError, RuntimeNotStartedError
from edgeguard.core.lifecycle import LifecycleManager, StateChangeHandler
from edgeguard.core.state import RuntimeState, StateMachine
from edgeguard.diagnostics.incidents import IncidentTracker
from edgeguard.diagnostics.timeline import EventLog
from edgeguard.durability.journal import IntentJournal
from edgeguard.durability.operations import (
    DurableFunc,
    ReplayHandler,
    build_durable_decorator,
    replay_pending,
)
from edgeguard.network.monitor import LayerCheck, NetworkLayer, NetworkMonitor
from edgeguard.persistence.database import Database
from edgeguard.process.supervisor import Supervisor, TaskFunc
from edgeguard.process.watchdog import Watchdog
from edgeguard.resilience.backoff import BackoffAlgorithm
from edgeguard.resilience.circuit_breaker import CircuitBreaker
from edgeguard.resilience.policy import build_reliable_decorator
from edgeguard.resilience.retry import RetryPolicy
from edgeguard.storage.monitor import Cleanup, StorageMonitor

logger = logging.getLogger("edgeguard.runtime")

P = ParamSpec("P")
T = TypeVar("T")


class _Startable(Protocol):
    """Structural type shared by every network/process/storage subsystem,
    so the runtime can auto-start/stop them without depending on their
    concrete classes."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class EdgeGuard:
    """Entry point for the edgeguard reliability runtime.

    Example:
        >>> guard = EdgeGuard("production-gateway", data_dir="/var/lib/edgeguard")
        >>> await guard.start()
        >>> ...
        >>> await guard.stop()

    Args:
        name: Identifier for this runtime instance, used in persisted state
            and log output. Should stay stable across restarts of the same
            logical device/application (e.g. ``"production-gateway"``), since
            later phases use it to locate this runtime's journal on disk.
        data_dir: Directory for local SQLite persistence. Created if it
            doesn't exist. Use a path on durable local storage, not tmpfs,
            so state survives reboots and power loss.
        recovery: Whether ``start()`` replays unfinished durable operations
            from the journal before the runtime becomes healthy. Disable
            only for tests or tools that need to inspect a journal without
            re-running its pending side effects.
    """

    def __init__(
        self,
        name: str,
        data_dir: str | Path = "./data",
        *,
        recovery: bool = True,
    ) -> None:
        if not name:
            raise ValueError("name must be a non-empty string")
        self._name = name
        self._data_dir = Path(data_dir)
        self._recovery_enabled = recovery

        self._states = StateMachine(RuntimeState.BOOTING)
        self._events = EventBus()
        self._state_change_handlers: list[StateChangeHandler] = []
        self._lifecycle = LifecycleManager(
            self._states,
            self._events,
            self._state_change_handlers,
        )
        self._database = Database(self._data_dir / f"{name}.sqlite3")
        self._journal = IntentJournal(self._database)
        self._timeline = EventLog(self._database)
        self._incidents = IncidentTracker()
        self._durable_handlers: dict[str, ReplayHandler] = {}
        self._subsystems: list[_Startable] = []
        self._watchdog_instance: Watchdog | None = None

        self._lock = asyncio.Lock()
        self._started = False
        self._stopped = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    @property
    def state(self) -> RuntimeState:
        """Current lifecycle state."""
        return self._states.current

    @property
    def database(self) -> Database:
        """The runtime's local SQLite handle, shared by other subsystems."""
        return self._database

    @property
    def events(self) -> EventBus:
        """The runtime's shared event bus."""
        return self._events

    @property
    def journal(self) -> IntentJournal:
        """The runtime's durable-operation journal, for direct inspection.

        Most applications never need this directly -- use
        :meth:`durable` instead -- but it's useful for e.g. a diagnostics
        CLI that wants to list intents without decorating anything.
        """
        return self._journal

    @property
    def timeline(self) -> EventLog:
        """The runtime's durable event history, for direct inspection.

        Recording starts partway through :meth:`start` (right after
        migrations run, before the boot sequence's own state-change events
        are published) and stops partway through :meth:`stop`, so every
        event published on :attr:`events` while the runtime is up is
        captured. Usable even before :meth:`start` -- e.g. by a diagnostics
        CLI querying a stopped runtime's on-disk database directly -- but
        it won't have recorded anything itself in that case.
        """
        return self._timeline

    @property
    def incidents(self) -> IncidentTracker:
        """Live tracker for spans of non-``HEALTHY`` time, attached and
        detached alongside :attr:`timeline`.

        For reconstructing incidents from a stopped runtime's durable
        history instead, see
        :func:`~edgeguard.diagnostics.incidents.build_incidents`.
        """
        return self._incidents

    def on_state_change(self, handler: StateChangeHandler) -> StateChangeHandler:
        """Register an async callback invoked on every lifecycle transition.

        Usable as a decorator::

            @guard.on_state_change
            async def handle_state_change(event: StateChangeEvent) -> None:
                print(event.previous, event.current)

        Handlers are awaited in registration order before the transition is
        considered complete, and a handler that raises is logged rather than
        propagated -- it cannot block or fail the transition it's observing.
        """
        self._state_change_handlers.append(handler)
        return handler

    def reliable(
        self,
        *,
        retries: int = 3,
        backoff: BackoffAlgorithm = "exponential",
        initial_delay: float = 1.0,
        max_delay: float = 300.0,
        jitter: bool = True,
        retry_on: tuple[type[Exception], ...] = (Exception,),
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
        circuit_breaker: bool | CircuitBreaker = False,
        name: str | None = None,
    ) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
        """Decorator composing timeout, retry/backoff, and an optional circuit
        breaker around an async function.

        Usable bare for sensible defaults::

            @guard.reliable()
            async def send_data(data):
                ...

        Or configured::

            @guard.reliable(retries=5, timeout=30, circuit_breaker=True)
            async def call_cloud():
                ...

        Args:
            retries: Total attempts (including the first), if ``retry`` is
                not given directly.
            backoff, initial_delay, max_delay, jitter, retry_on: Shorthand
                for building a :class:`~edgeguard.resilience.retry.RetryPolicy`;
                ignored if ``retry`` is given.
            retry: A fully-configured :class:`RetryPolicy`, for cases the
                shorthand parameters don't cover. Overrides the shorthand
                parameters above when given.
            timeout: Per-attempt timeout in seconds. ``None`` disables it.
            circuit_breaker: ``True`` creates a private breaker with default
                thresholds for this function; pass an existing
                :class:`~edgeguard.resilience.circuit_breaker.CircuitBreaker`
                instance to share one breaker across multiple functions that
                call the same downstream dependency. ``False`` (default)
                disables circuit breaking.
            name: Operation name used in events/logs. Defaults to the
                decorated function's ``__name__``.

        Retries wrap each attempt in the timeout; the circuit breaker (if
        any) wraps the whole retried operation, so a breaker trip means "this
        operation didn't succeed even after retrying", not "one attempt
        failed" -- see :mod:`edgeguard.resilience.policy` for the rationale.
        """
        return build_reliable_decorator(
            retries=retries,
            backoff=backoff,
            initial_delay=initial_delay,
            max_delay=max_delay,
            jitter=jitter,
            retry_on=retry_on,
            retry=retry,
            timeout=timeout,
            circuit_breaker=circuit_breaker,
            name=name,
            events=self._events,
            component=self._name,
        )

    def durable(
        self,
        operation: str,
        *,
        max_attempts: int | None = None,
    ) -> Callable[[DurableFunc[T]], DurableFunc[T]]:
        """Decorator making an async function's calls crash/reboot-safe.

        Every call is written to the local journal as a ``pending`` intent
        *before* the function runs. If the process dies or the device
        loses power mid-call, the intent survives on disk; the next time
        ``start()`` runs (with ``recovery=True``, the default) it calls the
        function again with the exact same arguments -- so applications get
        at-least-once execution across crashes without writing any
        recovery logic of their own::

            @guard.durable("publish_reading")
            async def publish_reading(sensor_id: str, value: float) -> None:
                ...

            await publish_reading(sensor_id="temp-1", value=21.5)

        The decorated function's arguments must be JSON-serializable and
        passed by a fixed name -- it must not declare ``*args`` or
        ``**kwargs``, since replay needs to reconstruct the original call
        by argument name from disk.

        Because at-least-once means the function may run more than once
        for the same logical call (e.g. it succeeds but the process dies
        before that's journaled), it must be safe to repeat -- writing to
        an idempotent API, using an idempotency key, or being naturally
        safe to redo.

        Decoration itself (registering the operation) works before
        ``start()``, same as ``reliable()`` -- but unlike ``reliable()``,
        *calling* the decorated function requires the runtime's database
        connection, so it raises
        :class:`~edgeguard.core.exceptions.RuntimeNotStartedError` if the
        runtime hasn't started yet.

        Args:
            operation: Stable name identifying this operation in the
                journal and in replay. Must be unique per ``EdgeGuard``
                instance; changing it later orphans any intents already
                recorded under the old name.
            max_attempts: Total attempts, counted across the original call
                and every replay, after which the intent is marked
                ``failed`` and left alone (raising
                :class:`~edgeguard.core.exceptions.DurableOperationExhaustedError`
                instead of retrying it again). ``None`` (default) retries
                forever, once per restart, until it succeeds.

        Raises:
            ValueError: if ``operation`` is already registered on this
                runtime.
        """
        return build_durable_decorator(
            operation=operation,
            journal=self._journal,
            registry=self._durable_handlers,
            max_attempts=max_attempts,
            events=self._events,
            component=self._name,
            is_started=lambda: self._started and not self._stopped,
        )

    def watch_network(
        self,
        checks: Mapping[NetworkLayer, LayerCheck],
        *,
        interval: float = 30.0,
    ) -> NetworkMonitor:
        """Create a :class:`~edgeguard.network.monitor.NetworkMonitor` wired
        to this runtime.

        Connectivity changes are published on the runtime's event bus and
        drive its lifecycle state (``HEALTHY``/``DEGRADED``/``OFFLINE``)
        automatically -- equivalent to constructing a ``NetworkMonitor``
        directly and passing ``events=guard.events``, ``component=guard.name``,
        and hand-wired ``set_state``/``get_state`` hooks.

        Like :meth:`durable`, calling this before :meth:`start` also makes
        the runtime start and stop the monitor's polling loop automatically
        alongside its own lifecycle; one created after :meth:`start` has
        already run must be started/stopped manually.

        Args:
            checks: See :class:`~edgeguard.network.monitor.NetworkMonitor`.
            interval: Seconds between polls once running.
        """
        monitor = NetworkMonitor(
            checks,
            interval=interval,
            events=self._events,
            component=self._name,
            set_state=self._set_runtime_state,
            get_state=lambda: self.state,
        )
        self._subsystems.append(monitor)
        return monitor

    def supervise(
        self,
        func: TaskFunc,
        *,
        name: str,
        max_crashes: int = 3,
        window: float = 60.0,
        backoff: BackoffAlgorithm = "exponential",
        initial_delay: float = 1.0,
        max_delay: float = 30.0,
        jitter: bool = True,
    ) -> Supervisor:
        """Create a :class:`~edgeguard.process.supervisor.Supervisor` wired
        to this runtime.

        A crash loop that exhausts ``max_crashes`` escalates this runtime to
        ``FAILED`` automatically. See :meth:`watch_network` for the
        registration-before-start contract that makes the supervisor start
        and stop with the runtime.

        Args:
            func, name, max_crashes, window, backoff, initial_delay,
            max_delay, jitter: See
                :class:`~edgeguard.process.supervisor.Supervisor`.
        """
        supervisor = Supervisor(
            func,
            name=name,
            max_crashes=max_crashes,
            window=window,
            backoff=backoff,
            initial_delay=initial_delay,
            max_delay=max_delay,
            jitter=jitter,
            events=self._events,
            component=self._name,
            set_state=self._set_runtime_state,
            get_state=lambda: self.state,
        )
        self._subsystems.append(supervisor)
        return supervisor

    @property
    def watchdog(self) -> Watchdog:
        """A :class:`~edgeguard.process.watchdog.Watchdog` shared by this
        runtime, wired the same way :meth:`watch_network` wires a monitor.

        Created (and registered for auto-start/stop, if accessed before
        :meth:`start`) the first time this property is read -- shared
        rather than one-per-call, since heartbeat targets are usually
        registered from several different places in an application and all
        want to escalate the same runtime.
        """
        if self._watchdog_instance is None:
            self._watchdog_instance = Watchdog(
                events=self._events,
                component=self._name,
                set_state=self._set_runtime_state,
                get_state=lambda: self.state,
            )
            self._subsystems.append(self._watchdog_instance)
        return self._watchdog_instance

    def watch_storage(
        self,
        path: str,
        *,
        low_water_bytes: int,
        low_water_inodes: int | None = None,
        cleanup: Sequence[Cleanup] = (),
        interval: float = 60.0,
    ) -> StorageMonitor:
        """Create a :class:`~edgeguard.storage.monitor.StorageMonitor` wired
        to this runtime.

        Low space moves this runtime to ``DEGRADED`` (back to ``HEALTHY`` on
        recovery); cleanup actions exhausting without freeing enough space
        escalates to ``FAILED``. See :meth:`watch_network` for the
        registration-before-start contract that makes the monitor start and
        stop with the runtime.

        Args:
            path, low_water_bytes, low_water_inodes, cleanup, interval: See
                :class:`~edgeguard.storage.monitor.StorageMonitor`.
        """
        monitor = StorageMonitor(
            path,
            low_water_bytes=low_water_bytes,
            low_water_inodes=low_water_inodes,
            cleanup=cleanup,
            interval=interval,
            events=self._events,
            component=self._name,
            set_state=self._set_runtime_state,
            get_state=lambda: self.state,
        )
        self._subsystems.append(monitor)
        return monitor

    async def _set_runtime_state(self, target: RuntimeState) -> None:
        await self._lifecycle.set_state(target, component=self._name)

    async def start(self) -> None:
        """Boot the runtime: open persistence, run migrations, become HEALTHY.

        Raises:
            RuntimeAlreadyStartedError: if the runtime is already running.
        """
        async with self._lock:
            if self._started and not self._stopped:
                raise RuntimeAlreadyStartedError(f"{self._name!r} is already started")
            self._data_dir.mkdir(parents=True, exist_ok=True)
            await self._database.connect()
            try:
                await self._database.migrate()
                # Attach after migrations (the events table must exist) but
                # before boot(), so even the BOOTING/INITIALIZING state-change
                # events are captured.
                self._timeline.attach(self._events)
                self._incidents.attach(self._events)
                await self._lifecycle.boot(on_init=self._on_init, component="runtime")
            except Exception:
                # Don't leak the open connection just because boot failed --
                # the runtime is FAILED, not "started", so nothing else will
                # call stop() to clean this up.
                self._timeline.detach(self._events)
                self._incidents.detach(self._events)
                await self._database.close()
                raise
            await self._database.save_runtime_state(self._name, self.state.value)
            self._started = True
            self._stopped = False
        logger.info("edgeguard runtime %r started", self._name)

    async def stop(self) -> None:
        """Gracefully shut down the runtime and close persistence.

        Idempotent: calling ``stop()`` on an already-stopped runtime is a
        no-op, since shutdown code paths (e.g. signal handlers) are prone to
        running more than once.

        Raises:
            RuntimeNotStartedError: if the runtime was never started.
        """
        async with self._lock:
            if not self._started:
                raise RuntimeNotStartedError(f"{self._name!r} was never started")
            if self._stopped:
                return
            await self._lifecycle.shutdown(on_stop=self._on_stop, component="runtime")
            self._timeline.detach(self._events)
            self._incidents.detach(self._events)
            await self._database.save_runtime_state(self._name, self.state.value)
            await self._database.close()
            self._stopped = True
        logger.info("edgeguard runtime %r stopped", self._name)

    async def _on_init(self) -> None:
        """Extension point for subsystem startup, awaited while INITIALIZING.

        Replays unfinished durable operations (see :meth:`durable`) unless
        ``recovery=False`` was passed to the constructor, then starts every
        network/process/storage subsystem registered via :meth:`watch_network`,
        :meth:`supervise`, :attr:`watchdog`, or :meth:`watch_storage` before
        this call. A subsystem that fails to start is treated the same as
        any other initialization failure -- it fails the boot rather than
        leaving the runtime looking healthy with a subsystem silently not
        running.
        """
        if self._recovery_enabled:
            await replay_pending(
                self._journal,
                self._durable_handlers,
                events=self._events,
                component=self._name,
            )
        for subsystem in self._subsystems:
            await subsystem.start()

    async def _on_stop(self) -> None:
        """Extension point for subsystem teardown, awaited while STOPPING.

        Stops every registered subsystem, in reverse registration order.
        One subsystem failing to stop cleanly is logged but must not
        prevent the others from being asked to, or stop the runtime from
        reaching ``STOPPED``.
        """
        for subsystem in reversed(self._subsystems):
            try:
                await subsystem.stop()
            except Exception:
                logger.exception("subsystem failed to stop cleanly")

    async def __aenter__(self) -> EdgeGuard:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()
