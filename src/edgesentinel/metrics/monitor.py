"""Hardware metrics monitoring: CPU load, memory pressure, and temperature.

Edge devices under sustained CPU/memory pressure or thermal throttling
degrade in ways :class:`~edgesentinel.network.monitor.NetworkMonitor` and
:class:`~edgesentinel.storage.monitor.StorageMonitor` can't see -- the network
and disk can both be fine while a stuck retry loop pegs the CPU, or the SoC
has throttled itself so hard the application effectively stalls.
``MetricsMonitor`` polls a configurable subset of CPU/memory/temperature
thresholds and runs a caller-supplied sequence of mitigation actions, in
order, whenever any of them is breached -- the same low/high-water-mark
shape as ``StorageMonitor``, mirrored here for "too much" instead of "too
little".

Metrics are read via an injectable check (see :mod:`edgesentinel.metrics.checks`
for the real, stdlib-based implementation) so the monitor itself has no
opinion about *how* usage is measured and can be driven entirely by fakes
in tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Sequence

from edgesentinel.core.events import Event, EventBus, Severity
from edgesentinel.core.exceptions import InvalidStateTransitionError
from edgesentinel.core.state import RuntimeState
from edgesentinel.metrics.checks import HardwareStatus, read_hardware_status

logger = logging.getLogger("edgesentinel.metrics")

MetricsCheck = Callable[[], HardwareStatus]
Mitigation = Callable[[], Awaitable[None]]
SetState = Callable[[RuntimeState], Awaitable[None]]
GetState = Callable[[], RuntimeState]
Sleep = Callable[[float], Awaitable[None]]

#: Runtime states the monitor is willing to move into/out of DEGRADED for a
#: plain high-usage warning. Boot, shutdown, and failure/recovery sequences
#: driven by other subsystems are never touched.
_MANAGED_STATES = frozenset({RuntimeState.HEALTHY, RuntimeState.DEGRADED})

#: States a monitor that has exhausted its mitigation actions is still
#: willing to escalate out of, to FAILED. A metrics monitor must never
#: fight the runtime's own shutdown sequence.
_UNSAFE_TO_ESCALATE = frozenset({RuntimeState.STOPPING, RuntimeState.STOPPED})


class MetricsMonitor:
    """Polls CPU/memory/temperature and runs scoped mitigation when high.

    Example:
        >>> monitor = MetricsMonitor(
        ...     cpu_high=0.9,
        ...     memory_high=0.9,
        ...     mitigations=[shed_background_work],
        ... )
        >>> status = await monitor.check_once()

    Args:
        cpu_high: Threshold for :attr:`HardwareStatus.cpu_load_ratio` above
            which usage counts as high. ``None`` disables the CPU check.
        memory_high: Threshold for :attr:`HardwareStatus.memory_used_ratio`.
            ``None`` disables the memory check.
        temperature_high_celsius: Threshold for
            :attr:`HardwareStatus.temperature_celsius`. ``None`` disables
            the temperature check -- also skipped if the check function
            itself reports no temperature (e.g. no thermal zone available).
        mitigations: Async, zero-argument callables run in order, one at a
            time, re-checking every metric after each, whenever any
            threshold is breached. Stops early the moment nothing is high
            any more -- order them from least to most aggressive (e.g.
            shed background work before killing a subprocess). A
            mitigation action that raises is logged and skipped rather
            than aborting the rest of the sequence.
        interval: Seconds between polls once :meth:`start` is running.
        events / component: Where high/recovered/mitigation events are
            published.
        set_state / get_state: Optional hooks letting the monitor drive the
            runtime's lifecycle state: DEGRADED while high, back to
            HEALTHY on recovery, and FAILED if mitigation runs out without
            bringing every metric back under its threshold. Both or
            neither. The monitor never touches states it doesn't own for
            the DEGRADED/HEALTHY swing -- see :data:`_MANAGED_STATES` --
            though a mitigation-exhausted escalation to FAILED follows the
            same rule other subsystems use, see :data:`_UNSAFE_TO_ESCALATE`.
        metrics_check: Injectable metrics check, for deterministic tests.
        sleep: Injectable sleep function for the polling loop.

    Raises:
        ValueError: if none of ``cpu_high``/``memory_high``/
            ``temperature_high_celsius`` is given, any given threshold is
            not positive, ``interval`` is not positive, or only one of
            ``set_state``/``get_state`` is given.
    """

    def __init__(
        self,
        *,
        cpu_high: float | None = None,
        memory_high: float | None = None,
        temperature_high_celsius: float | None = None,
        mitigations: Sequence[Mitigation] = (),
        interval: float = 30.0,
        events: EventBus | None = None,
        component: str = "metrics",
        set_state: SetState | None = None,
        get_state: GetState | None = None,
        metrics_check: MetricsCheck = read_hardware_status,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if cpu_high is None and memory_high is None and temperature_high_celsius is None:
            raise ValueError(
                "at least one of cpu_high, memory_high, temperature_high_celsius must be given"
            )
        if cpu_high is not None and cpu_high <= 0:
            raise ValueError(f"cpu_high must be > 0, got {cpu_high}")
        if memory_high is not None and memory_high <= 0:
            raise ValueError(f"memory_high must be > 0, got {memory_high}")
        if temperature_high_celsius is not None and temperature_high_celsius <= 0:
            raise ValueError(
                f"temperature_high_celsius must be > 0, got {temperature_high_celsius}"
            )
        if interval <= 0:
            raise ValueError(f"interval must be > 0, got {interval}")
        if (set_state is None) != (get_state is None):
            raise ValueError("set_state and get_state must be given together, or neither")
        self._cpu_high = cpu_high
        self._memory_high = memory_high
        self._temperature_high_celsius = temperature_high_celsius
        self._mitigations = tuple(mitigations)
        self._interval = interval
        self._events = events
        self._component = component
        self._set_state = set_state
        self._get_state = get_state
        self._metrics_check = metrics_check
        self._sleep = sleep
        self._status: HardwareStatus | None = None
        self._high = False
        self._task: asyncio.Task[None] | None = None

    @property
    def status(self) -> HardwareStatus | None:
        """The most recent poll result, or ``None`` before the first poll."""
        return self._status

    @property
    def is_high(self) -> bool:
        """Whether any configured metric was above its threshold as of the
        last poll."""
        return self._high

    def _is_high(self, status: HardwareStatus) -> bool:
        if self._cpu_high is not None and status.cpu_load_ratio >= self._cpu_high:
            return True
        if self._memory_high is not None and status.memory_used_ratio >= self._memory_high:
            return True
        return (
            self._temperature_high_celsius is not None
            and status.temperature_celsius is not None
            and status.temperature_celsius >= self._temperature_high_celsius
        )

    def _metadata(self, status: HardwareStatus) -> dict[str, object]:
        return {
            "cpu_load_ratio": status.cpu_load_ratio,
            "memory_used_ratio": status.memory_used_ratio,
            "temperature_celsius": status.temperature_celsius,
        }

    async def check_once(self) -> HardwareStatus:
        """Read metrics once, running mitigation and publishing events on
        high/recovered transitions.

        Returns the status observed *after* mitigation ran, if it ran -- so
        a caller can tell whether mitigation actually brought usage back
        down.
        """
        self._status = self._metrics_check()
        was_high = self._high
        self._high = self._is_high(self._status)
        if self._high and not was_high:
            await self._on_high()
        elif not self._high and was_high:
            await self._on_recovered()
        return self._status

    async def _on_high(self) -> None:
        assert self._status is not None
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="hardware_metrics_high",
                    component=self._component,
                    severity=Severity.WARNING,
                    metadata=self._metadata(self._status),
                )
            )
        if self._set_state is not None:
            await self._apply_to_runtime(RuntimeState.DEGRADED)
        await self._run_mitigations()
        if not self._high:
            await self._on_recovered()
        elif self._mitigations:
            # Only escalate for actually exhausting configured mitigations
            # -- a monitor with none configured is a plain DEGRADED
            # observer, not one that's "given up".
            await self._on_mitigations_exhausted()

    async def _run_mitigations(self) -> None:
        for action in self._mitigations:
            if not self._high:
                return
            try:
                await action()
            except Exception:
                logger.exception("hardware metrics mitigation action raised")
            self._status = self._metrics_check()
            self._high = self._is_high(self._status)

    async def _on_mitigations_exhausted(self) -> None:
        assert self._status is not None
        logger.error(
            "hardware metrics still high after running %d mitigation action(s): "
            "cpu_load_ratio=%.2f memory_used_ratio=%.2f temperature_celsius=%s",
            len(self._mitigations),
            self._status.cpu_load_ratio,
            self._status.memory_used_ratio,
            self._status.temperature_celsius,
        )
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="hardware_metrics_mitigation_exhausted",
                    component=self._component,
                    severity=Severity.CRITICAL,
                    metadata=self._metadata(self._status),
                )
            )
        if self._set_state is not None:
            await self._escalate()

    async def _on_recovered(self) -> None:
        assert self._status is not None
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="hardware_metrics_recovered",
                    component=self._component,
                    metadata=self._metadata(self._status),
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
                logger.exception("hardware metrics monitor poll iteration failed")

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
                "hardware metrics monitor could not move runtime to %s: no "
                "longer reachable from the current state",
                target.value,
            )
        except Exception:
            logger.exception("hardware metrics monitor failed to update runtime state")

    async def _escalate(self) -> None:
        assert self._set_state is not None
        assert self._get_state is not None
        if self._get_state() in _UNSAFE_TO_ESCALATE:
            return
        try:
            await self._set_state(RuntimeState.FAILED)
        except InvalidStateTransitionError:
            logger.debug(
                "hardware metrics monitor could not move runtime to FAILED: "
                "no longer reachable from the current state"
            )
        except Exception:
            logger.exception("hardware metrics monitor failed to update runtime state")
