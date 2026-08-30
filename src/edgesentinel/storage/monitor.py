"""Free-space and inode monitoring with scoped cleanup.

Edge devices run on flash storage that fills up silently until every write
starts failing -- often with the application's own logs as the culprit.
``StorageMonitor`` polls free bytes (and, optionally, free inodes -- a
filesystem can refuse every new file while technically not "full" if it's
run out of those) on a configured path, and runs a caller-supplied sequence
of cleanup actions, in order, whenever usage drops below a low-water mark.

Usage is read via an injectable check (see :mod:`edgesentinel.storage.checks`
for the real, stdlib-based implementation) so the monitor itself has no
opinion about *how* free space is measured and can be driven entirely by
fakes in tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Sequence

from edgesentinel.core.events import Event, EventBus, Severity
from edgesentinel.core.exceptions import InvalidStateTransitionError
from edgesentinel.core.state import RuntimeState
from edgesentinel.storage.checks import StorageStatus, disk_usage

logger = logging.getLogger("edgesentinel.storage")

UsageCheck = Callable[[str], StorageStatus]
Cleanup = Callable[[], Awaitable[None]]
SetState = Callable[[RuntimeState], Awaitable[None]]
GetState = Callable[[], RuntimeState]
Sleep = Callable[[float], Awaitable[None]]

#: Runtime states the monitor is willing to move into/out of DEGRADED for a
#: plain low-space warning. Boot, shutdown, and failure/recovery sequences
#: driven by other subsystems are never touched.
_MANAGED_STATES = frozenset({RuntimeState.HEALTHY, RuntimeState.DEGRADED})

#: States a monitor that has exhausted its cleanup actions is still willing
#: to escalate out of, to FAILED. A storage monitor must never fight the
#: runtime's own shutdown sequence.
_UNSAFE_TO_ESCALATE = frozenset({RuntimeState.STOPPING, RuntimeState.STOPPED})


class StorageMonitor:
    """Polls free space on ``path`` and runs scoped cleanup when it's low.

    Example:
        >>> monitor = StorageMonitor(
        ...     "/var/lib/myapp",
        ...     low_water_bytes=100 * 1024 * 1024,
        ...     cleanup=[delete_old_logs, delete_old_snapshots],
        ... )
        >>> status = await monitor.check_once()

    Args:
        path: Filesystem path to monitor. Usage is reported per-filesystem,
            not per-directory, so any path on the target filesystem works.
        low_water_bytes: Free-byte threshold below which storage counts as
            low.
        low_water_inodes: Optional free-inode threshold. Most deployments
            won't need this, but it's cheap insurance for ones that create
            many small files, since a filesystem can be out of inodes while
            still reporting free bytes.
        cleanup: Async, zero-argument callables run in order, one at a
            time, re-checking usage after each, whenever storage goes low.
            Stops early the moment usage is no longer low -- order them
            from least to most aggressive (e.g. rotate logs before deleting
            old snapshots). A cleanup action that raises is logged and
            skipped rather than aborting the rest of the sequence.
        interval: Seconds between polls once :meth:`start` is running.
        events / component: Where low/recovered/cleanup events are
            published.
        set_state / get_state: Optional hooks letting the monitor drive the
            runtime's lifecycle state: DEGRADED while low, back to HEALTHY
            on recovery, and FAILED if cleanup runs out without freeing
            enough space. Both or neither. The monitor never touches states
            it doesn't own for the DEGRADED/HEALTHY swing -- see
            :data:`_MANAGED_STATES` -- though a cleanup-exhausted escalation
            to FAILED follows the same rule other subsystems use, see
            :data:`_UNSAFE_TO_ESCALATE`.
        usage_check: Injectable usage check, for deterministic tests.
        sleep: Injectable sleep function for the polling loop.

    Raises:
        ValueError: if ``low_water_bytes``/``low_water_inodes``/``interval``
            is not positive, or if only one of ``set_state``/``get_state``
            is given.
    """

    def __init__(
        self,
        path: str,
        *,
        low_water_bytes: int,
        low_water_inodes: int | None = None,
        cleanup: Sequence[Cleanup] = (),
        interval: float = 60.0,
        events: EventBus | None = None,
        component: str = "storage",
        set_state: SetState | None = None,
        get_state: GetState | None = None,
        usage_check: UsageCheck = disk_usage,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if low_water_bytes <= 0:
            raise ValueError(f"low_water_bytes must be > 0, got {low_water_bytes}")
        if low_water_inodes is not None and low_water_inodes <= 0:
            raise ValueError(f"low_water_inodes must be > 0, got {low_water_inodes}")
        if interval <= 0:
            raise ValueError(f"interval must be > 0, got {interval}")
        if (set_state is None) != (get_state is None):
            raise ValueError("set_state and get_state must be given together, or neither")
        self._path = path
        self._low_water_bytes = low_water_bytes
        self._low_water_inodes = low_water_inodes
        self._cleanup = tuple(cleanup)
        self._interval = interval
        self._events = events
        self._component = component
        self._set_state = set_state
        self._get_state = get_state
        self._usage_check = usage_check
        self._sleep = sleep
        self._status: StorageStatus | None = None
        self._low = False
        self._task: asyncio.Task[None] | None = None

    @property
    def status(self) -> StorageStatus | None:
        """The most recent poll result, or ``None`` before the first poll."""
        return self._status

    @property
    def is_low(self) -> bool:
        """Whether storage was below a low-water mark as of the last poll."""
        return self._low

    def _is_low(self, status: StorageStatus) -> bool:
        if status.free_bytes < self._low_water_bytes:
            return True
        return (
            self._low_water_inodes is not None
            and status.free_inodes is not None
            and status.free_inodes < self._low_water_inodes
        )

    async def check_once(self) -> StorageStatus:
        """Read usage once, running cleanup and publishing events on
        low/recovered transitions.

        Returns the status observed *after* cleanup ran, if it ran -- so a
        caller can tell whether cleanup actually freed enough space.
        """
        self._status = self._usage_check(self._path)
        was_low = self._low
        self._low = self._is_low(self._status)
        if self._low and not was_low:
            await self._on_low()
        elif not self._low and was_low:
            await self._on_recovered()
        return self._status

    async def _on_low(self) -> None:
        assert self._status is not None
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="storage_low",
                    component=self._component,
                    severity=Severity.WARNING,
                    metadata={"path": self._path, "free_bytes": self._status.free_bytes},
                )
            )
        if self._set_state is not None:
            await self._apply_to_runtime(RuntimeState.DEGRADED)
        await self._run_cleanup()
        if not self._low:
            await self._on_recovered()
        elif self._cleanup:
            # Only escalate for actually exhausting configured cleanup --
            # a monitor with none configured is a plain DEGRADED observer,
            # not one that's "given up".
            await self._on_cleanup_exhausted()

    async def _run_cleanup(self) -> None:
        for action in self._cleanup:
            if not self._low:
                return
            try:
                await action()
            except Exception:
                logger.exception("storage cleanup action raised")
            self._status = self._usage_check(self._path)
            self._low = self._is_low(self._status)

    async def _on_cleanup_exhausted(self) -> None:
        assert self._status is not None
        logger.error(
            "storage on %r still low after running %d cleanup action(s): %d bytes free",
            self._path,
            len(self._cleanup),
            self._status.free_bytes,
        )
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="storage_cleanup_exhausted",
                    component=self._component,
                    severity=Severity.CRITICAL,
                    metadata={"path": self._path, "free_bytes": self._status.free_bytes},
                )
            )
        if self._set_state is not None:
            await self._escalate()

    async def _on_recovered(self) -> None:
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="storage_recovered",
                    component=self._component,
                    metadata={"path": self._path},
                )
            )
        if self._set_state is not None:
            await self._apply_to_runtime(RuntimeState.HEALTHY)

    async def start(self) -> None:
        """Run an initial check and start polling every ``interval``
        seconds. Safe to call more than once; a no-op while already
        running."""
        if self._task is not None:
            return
        await self.check_once()
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Stop polling. Safe to call more than once, or if never started."""
        if self._task is None:
            return
        task, self._task = self._task, None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _poll_loop(self) -> None:
        while True:
            await self._sleep(self._interval)
            try:
                await self.check_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("storage monitor poll iteration failed")

    async def _apply_to_runtime(self, target: RuntimeState) -> None:
        assert self._set_state is not None
        assert self._get_state is not None
        state = self._get_state()
        if state not in _MANAGED_STATES or target is state:
            return
        try:
            await self._set_state(target)
        except InvalidStateTransitionError:
            logger.debug(
                "storage monitor could not move runtime to %s: no longer "
                "reachable from the current state",
                target.value,
            )
        except Exception:
            logger.exception("storage monitor failed to update runtime state")

    async def _escalate(self) -> None:
        assert self._set_state is not None
        assert self._get_state is not None
        if self._get_state() in _UNSAFE_TO_ESCALATE:
            return
        try:
            await self._set_state(RuntimeState.FAILED)
        except InvalidStateTransitionError:
            logger.debug(
                "storage monitor could not move runtime to FAILED: no "
                "longer reachable from the current state"
            )
        except Exception:
            logger.exception("storage monitor failed to update runtime state")
