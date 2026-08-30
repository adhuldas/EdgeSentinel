"""Layered connectivity monitoring.

A device can lose connectivity at different layers independently: the
network interface itself can be up while the gateway is unreachable, DNS
can be broken while raw IP routing still works, and so on. ``NetworkMonitor``
checks a configurable subset of these layers bottom-up and stops at the
first failure, since a higher layer can never be meaningfully "up" while a
lower one it depends on is down.

Checks are supplied by the caller (see :mod:`edgeguard.network.checks` for
stdlib-based building blocks) so the monitor itself has no opinion about
*how* connectivity is tested and can be driven entirely by fakes in tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import enum
import logging
from collections.abc import Awaitable, Callable, Mapping

from edgeguard.core.events import Event, EventBus, Severity
from edgeguard.core.exceptions import InvalidStateTransitionError
from edgeguard.core.state import RuntimeState

logger = logging.getLogger("edgeguard.network")


class NetworkLayer(enum.Enum):
    """Connectivity layers, checked in this order (lowest first)."""

    LINK = "link"
    GATEWAY = "gateway"
    DNS = "dns"
    INTERNET = "internet"


#: Fixed check order -- a layer only means something if every layer below
#: it already passed, so :meth:`NetworkMonitor.check_once` stops here.
_LAYER_ORDER: tuple[NetworkLayer, ...] = (
    NetworkLayer.LINK,
    NetworkLayer.GATEWAY,
    NetworkLayer.DNS,
    NetworkLayer.INTERNET,
)

LayerCheck = Callable[[], Awaitable[bool]]
SetState = Callable[[RuntimeState], Awaitable[None]]
GetState = Callable[[], RuntimeState]
Sleep = Callable[[float], Awaitable[None]]

#: Runtime states the monitor is willing to move the runtime out of. Boot,
#: shutdown, failure, and process-driven recovery are owned by other parts
#: of the runtime -- a network blip must never override those.
_MANAGED_STATES = frozenset({RuntimeState.HEALTHY, RuntimeState.DEGRADED, RuntimeState.OFFLINE})


@dataclasses.dataclass(frozen=True, slots=True)
class NetworkStatus:
    """Result of one :meth:`NetworkMonitor.check_once` poll.

    ``reachable`` is the ordered prefix of :data:`_LAYER_ORDER` that passed
    on this poll -- checking stops at the first configured layer that
    fails, so e.g. ``(LINK, GATEWAY)`` means DNS either failed or was never
    reached because GATEWAY failed first.
    """

    reachable: tuple[NetworkLayer, ...] = ()

    @property
    def highest_layer(self) -> NetworkLayer | None:
        """The furthest layer confirmed reachable, or ``None`` if even the
        first configured layer failed."""
        return self.reachable[-1] if self.reachable else None

    def is_reachable(self, layer: NetworkLayer) -> bool:
        return layer in self.reachable


class NetworkMonitor:
    """Polls a set of layered connectivity checks and tracks the result.

    Example:
        >>> monitor = NetworkMonitor({
        ...     NetworkLayer.DNS: lambda: dns_resolves("example.com"),
        ...     NetworkLayer.INTERNET: lambda: tcp_reachable("1.1.1.1", 443),
        ... })
        >>> status = await monitor.check_once()

    Args:
        checks: Async, zero-argument callables keyed by the layer they test.
            Only configured layers are evaluated; skip a layer you don't
            want to distinguish (e.g. configure only DNS and INTERNET).
        interval: Seconds between polls once :meth:`start` is running.
        events: Event bus to publish ``network_status_changed`` on, when
            the highest reachable layer changes.
        component: Component name attached to published events.
        set_state / get_state: Optional hooks letting the monitor drive the
            runtime's lifecycle state (HEALTHY / DEGRADED / OFFLINE) as
            connectivity changes. Both must be given together, or neither.
            The monitor never touches states it doesn't own -- see
            :data:`_MANAGED_STATES` -- so it can't interfere with boot,
            shutdown, or a process supervisor's FAILED/RECOVERING sequence.
        sleep: Injectable sleep function for the polling loop, for
            deterministic tests.
    """

    def __init__(
        self,
        checks: Mapping[NetworkLayer, LayerCheck],
        *,
        interval: float = 30.0,
        events: EventBus | None = None,
        component: str = "network",
        set_state: SetState | None = None,
        get_state: GetState | None = None,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if not checks:
            raise ValueError("checks must contain at least one layer")
        if interval <= 0:
            raise ValueError(f"interval must be > 0, got {interval}")
        if (set_state is None) != (get_state is None):
            raise ValueError("set_state and get_state must be given together, or neither")
        self._checks = dict(checks)
        self._interval = interval
        self._events = events
        self._component = component
        self._set_state = set_state
        self._get_state = get_state
        self._sleep = sleep
        self._status: NetworkStatus | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def status(self) -> NetworkStatus | None:
        """The most recent poll result, or ``None`` before the first poll."""
        return self._status

    @property
    def is_fully_connected(self) -> bool:
        """Whether every configured layer was reachable on the last poll."""
        return self._status is not None and len(self._status.reachable) == len(self._checks)

    async def check_once(self) -> NetworkStatus:
        """Run every configured check once and return the resulting status.

        A check that raises is treated as a failure at that layer (logged,
        not propagated) rather than crashing the poll -- a broken check
        must never be indistinguishable from a crashed monitor.
        """
        reachable: list[NetworkLayer] = []
        for layer in _LAYER_ORDER:
            check = self._checks.get(layer)
            if check is None:
                continue
            try:
                ok = await check()
            except Exception:
                logger.exception("connectivity check for layer %s raised", layer.value)
                ok = False
            if not ok:
                break
            reachable.append(layer)
        previous = self._status
        current = NetworkStatus(reachable=tuple(reachable))
        self._status = current
        if previous is None or previous.highest_layer != current.highest_layer:
            await self._on_change(current)
        return current

    async def start(self) -> None:
        """Run an initial check and start polling every ``interval`` seconds.

        Safe to call more than once; subsequent calls are no-ops while
        already running.
        """
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
                logger.exception("network monitor poll iteration failed")

    async def _on_change(self, current: NetworkStatus) -> None:
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="network_status_changed",
                    component=self._component,
                    severity=Severity.INFO if self.is_fully_connected else Severity.WARNING,
                    metadata={
                        "current_layer": (
                            current.highest_layer.value if current.highest_layer else None
                        ),
                        "fully_connected": self.is_fully_connected,
                    },
                )
            )
        if self._set_state is not None:
            await self._apply_to_runtime(current)

    async def _apply_to_runtime(self, current: NetworkStatus) -> None:
        assert self._set_state is not None
        assert self._get_state is not None
        state = self._get_state()
        if state not in _MANAGED_STATES:
            return
        if self.is_fully_connected:
            target = RuntimeState.HEALTHY
        elif current.highest_layer is None:
            target = RuntimeState.OFFLINE
        else:
            target = RuntimeState.DEGRADED
        if target is state:
            return
        try:
            # OFFLINE -> HEALTHY isn't a legal single hop; recovering from a
            # full outage always passes through DEGRADED first.
            if target is RuntimeState.HEALTHY and state is RuntimeState.OFFLINE:
                await self._set_state(RuntimeState.DEGRADED)
            await self._set_state(target)
        except InvalidStateTransitionError:
            logger.debug(
                "network monitor could not move runtime to %s: no longer "
                "reachable from the current state",
                target.value,
            )
        except Exception:
            logger.exception("network monitor failed to update runtime state")
