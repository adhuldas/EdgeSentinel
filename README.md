# edgeguard

**A reliability runtime for Linux edge devices.**

> Applications describe *what* they want to happen. edgeguard handles *how*
> that survives failure.

[![CI](https://github.com/edgeguard/edgeguard/actions/workflows/ci.yml/badge.svg)](https://github.com/edgeguard/edgeguard/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![status](https://img.shields.io/badge/status-alpha%20%E2%80%94%20phase%207-orange)

---

## The problem

Code running on a Raspberry Pi, an industrial gateway, or an ARM edge box
fails differently than code running in a cloud region:

- The network doesn't just "have latency" -- it disappears for minutes, DNS
  breaks independently of the link, and MQTT brokers drop silently.
- The process doesn't get gracefully drained -- it gets OOM-killed, the
  device loses power mid-write, or someone power-cycles it in the field.
- There's no ops team watching a dashboard. If an operation was half-done
  when the power died, *something on the device itself* has to notice that
  on the next boot and decide what to do about it.
- Disk is finite and often on flash storage that wears out if you thrash it
  with logs.

Retry libraries help with one slice of this (a flaky call). They don't help
with the rest: knowing whether you're offline at the network layer, the DNS
layer, or the service layer; recovering an operation that was interrupted by
a reboot; not blowing the SD card up with log writes while retrying;
noticing that a supervised process is stuck in a crash loop instead of
restarting it forever.

## What edgeguard is

edgeguard is a small, asyncio-first runtime you embed in your edge
application. You tell it what durable operations, retry policies, and
supervised processes you have; it tracks device and network health, persists
enough state locally to survive a crash or power loss, and gives you a
timeline of what happened when things went wrong.

```python
from edgeguard import EdgeGuard

guard = EdgeGuard("production-gateway", data_dir="/var/lib/edgeguard")

await guard.start()


@guard.on_state_change
async def handle_state_change(event):
    print(event.previous, "->", event.current)


...

await guard.stop()
```

## How this differs from a retry library

| | Retry library | edgeguard |
|---|---|---|
| Scope | One function call | Whole application lifecycle |
| Network awareness | None (just fails and retries) | Link / gateway / DNS / internet / service, as separate layers |
| Survives a reboot | No | Durable operations replay from a local journal on boot |
| Process supervision | No | Detects crash loops, escalates instead of restarting forever |
| Storage awareness | No | Monitors free space / inodes, scoped cleanup policies |
| Observability | Whatever you bolt on | Built-in event timeline and incident reports |

edgeguard uses retry, backoff, and circuit breakers internally -- they're
necessary, not sufficient. It is not a replacement for `tenacity`; it's the
layer above that decides *when* to retry, *what* to do if retrying never
works, and *how* to prove afterwards what happened.

## Who should use this

Engineers building applications that run unattended on Linux edge hardware
(Raspberry Pi, Jetson, industrial PCs, ARM/x86 gateways, Docker-based edge
deployments) and need them to keep working -- or fail safely and recover on
their own -- without a human nearby to restart things.

edgeguard complements `systemd`, it doesn't replace it. Use `systemd` (or
your container runtime) to keep *your process* running; use edgeguard inside
that process to keep *your application's operations* correct across network
loss, dependency failure, and crashes.

## Architecture

```
APPLICATION
     |
  EDGEGUARD
     |
RELIABLE EXECUTION  ->  FAILURE DETECTION  ->  RECOVERY  ->  DIAGNOSTICS
```

```
edgeguard/
├── core/          runtime, lifecycle state machine, events           [Phase 1 - done]
├── persistence/   SQLite storage, migrations, journal                [Phase 1 & 3 - done]
├── resilience/    retry, backoff, timeout, circuit breaker           [Phase 2 - done]
├── durability/    intent journal, durable operations, replay         [Phase 3 - done]
├── process/       supervisor, watchdog, health checks                [Phase 4 - done]
├── network/       layered connectivity (link/gateway/DNS/internet)   [Phase 4 - done]
├── storage/       free-space monitoring, scoped cleanup              [Phase 4 - done]
├── diagnostics/   event timeline, incidents, reports                 [Phase 5 - done]
├── integrations/  MQTT, HTTP (optional extras)                       [Phase 6 - done]
├── metrics/       CPU/memory/temperature monitoring, mitigation      [Phase 7 - done]
└── cli/           edgeguard status / timeline / incidents            [Phase 5 - done]
```

## Development status

edgeguard is built in phases, each one fully tested before the next begins.
**This is Phase 7.** See `CHANGELOG.md` for exactly what exists today.

Implemented and tested:

- `EdgeGuard` runtime: `start()` / `stop()` / async context manager.
- A strongly-typed lifecycle state machine (`BOOTING` -> `INITIALIZING` ->
  `HEALTHY` / `DEGRADED` / `OFFLINE` / `RECOVERING` / `FAILED` -> `STOPPING`
  -> `STOPPED`) that rejects invalid transitions.
  `@guard.on_state_change` for observing every transition.
- An async event bus that later subsystems (diagnostics, metrics) plug into.
- Local SQLite persistence (WAL mode, versioned migrations, transactions)
  with the runtime's own state persisted across restarts.
- Backoff algorithms (fixed, linear, exponential) with optional full jitter,
  and a `RetryPolicy` for retrying an async operation with configurable
  attempts, exception filtering, and an `on_retry` hook.
- Per-attempt timeouts (`OperationTimeoutError` on expiry).
- A `CircuitBreaker` (CLOSED/OPEN/HALF_OPEN) with a single concurrency-safe
  HALF_OPEN trial and optional state persistence that survives a restart.
- `guard.reliable()`: a decorator composing circuit breaker, retry/backoff,
  and per-attempt timeout around an async function, publishing events onto
  the runtime's event bus as it retries or resolves.
- A write-ahead intent journal (SQLite-backed): every `guard.durable(...)`
  call is recorded as a `pending` intent *before* it runs, so a crash or
  reboot mid-call leaves a durable record instead of silently losing the
  operation.
- `guard.durable(operation)`: a decorator giving an async function
  at-least-once, crash/reboot-safe execution semantics. Registering the
  operation works before `start()`, same as `reliable()`; calling it
  requires the runtime to be started, since it must write to the journal
  first.
- Startup replay: `start()` replays every intent left `pending` or
  `in_progress` by a previous run before the runtime becomes healthy. One
  intent failing (or exhausting `max_attempts`) never blocks the rest of
  the journal or the runtime from starting; unregistered operations are
  left pending and reported via a `durable_operation_unhandled` event.
- `NetworkMonitor`: polls a configurable subset of LINK/GATEWAY/DNS/INTERNET
  checks bottom-up, stopping at the first failing layer, and can drive the
  runtime to `HEALTHY`/`DEGRADED`/`OFFLINE` as connectivity changes.
  `tcp_reachable()`/`dns_resolves()` ship as stdlib-only building blocks.
- `Supervisor`: restarts a crashed or exited in-process async task with
  backoff, giving up (and, if wired to the runtime, escalating to `FAILED`)
  after too many crashes within a sliding window instead of restarting a
  broken task forever.
- `Watchdog`: heartbeat-based staleness detection for tasks that hang
  instead of crashing -- anything that stops checking in within its
  timeout is reported stale and can escalate the runtime to `FAILED`.
- `StorageMonitor`: polls free bytes (and, optionally, free inodes) on a
  path and runs scoped cleanup actions in order when usage drops below a
  low-water mark, moving the runtime to `DEGRADED` while low and escalating
  to `FAILED` if cleanup runs out without freeing enough space.
- `guard.watch_network()`, `guard.supervise()`, `guard.watchdog`, and
  `guard.watch_storage()`: factory methods wiring each subsystem above to
  the runtime's event bus and lifecycle state. Registering before `start()`
  makes the runtime start and stop the subsystem automatically, same as
  `guard.durable(...)`.
- `guard.timeline`: an `EventLog` attached to the runtime's event bus from
  right after `start()` runs migrations until `stop()` closes the
  database, durably recording every event -- including boot/shutdown
  transitions -- to a local `events` table. Queryable by component, type,
  minimum severity, and time range, from a live runtime or (via a
  standalone `Database`/`EventLog`) a stopped one.
- `guard.incidents`: an `IncidentTracker` grouping spans of non-`HEALTHY`
  time into `Incident` records from the same `state_change` events,
  regardless of which Phase 4 subsystem triggered them. `build_incidents()`
  reconstructs the same incidents offline from a persisted timeline, for
  inspecting a runtime that isn't currently running.
- The `edgeguard` CLI: `edgeguard --name <name> --data-dir <dir>
  status|timeline|incidents`, reading a runtime's on-disk SQLite database
  directly -- no live runtime process required, thanks to WAL mode.
- `HttpEventPublisher` (`edgeguard.integrations.http`): forwards events as
  JSON POSTs to a webhook URL. Stdlib-only (`urllib` wrapped in
  `asyncio.to_thread`), so no extra dependency is needed to use it.
- `MqttPublisher` (`edgeguard.integrations.mqtt`): forwards events to an
  MQTT broker. Talks to a small structural `MqttClient` protocol rather
  than a concrete library, so the module itself has no import-time
  dependency; connecting for real needs the `paho-mqtt` package (`pip
  install edgeguard[mqtt]`).
- Both integrations support `min_severity` filtering and attach/detach the
  same way `EventLog` and `IncidentTracker` do, and never let a publish
  failure (network error, bad status, broker down) propagate and crash the
  reliability path they're observing.
- `MetricsMonitor` (`edgeguard.metrics`): polls CPU load average, memory
  pressure, and (where available) SoC temperature, reading them via
  stdlib-only, injectable checks (`os.getloadavg`, `/proc/meminfo`, a Linux
  thermal zone) -- no `psutil` dependency. Same low/high-water-mark shape
  as `StorageMonitor`: any configured threshold being crossed runs a
  caller-supplied sequence of mitigation actions, in order, stopping once
  usage is back down; exhausting them without recovering escalates the
  runtime the same way cleanup exhaustion does for storage.
- `guard.watch_hardware()`: factory method wiring a `MetricsMonitor` to the
  runtime's event bus and lifecycle state, same registration-before-start
  contract as `guard.watch_network()` and `guard.watch_storage()`.

Anything described elsewhere in this repository's design docs that isn't
listed under "Implemented" above is a design target, not shipped behavior.

## Installation

```bash
pip install edgeguard          # not yet published -- see status above
```

For local development, see `CONTRIBUTING.md`.

```bash
pip install -e ".[dev]"
```

## Getting started

```python
import asyncio
from edgeguard import EdgeGuard, RuntimeState


async def main() -> None:
    guard = EdgeGuard("my-device", data_dir="./data")

    @guard.on_state_change
    async def on_change(event):
        print(f"{event.previous.value} -> {event.current.value}")

    async with guard:
        assert guard.state is RuntimeState.HEALTHY

        @guard.reliable(retries=5, timeout=10, circuit_breaker=True)
        async def publish_reading(value: float) -> None:
            # ... call a flaky downstream service ...
            ...

        await publish_reading(21.5)


asyncio.run(main())
```

`reliable()` wraps the decorated function with a circuit breaker (outermost),
retry/backoff (middle), and a per-attempt timeout (innermost): a breaker trip
means "this operation isn't succeeding even after retrying", not "one
attempt failed", so a single flaky call never trips the breaker by itself.

```python
guard = EdgeGuard("my-device", data_dir="./data")


@guard.durable("publish_reading")
async def publish_reading(sensor_id: str, value: float) -> None:
    # ... call a downstream service ...
    ...


async def main() -> None:
    async with guard:
        await publish_reading(sensor_id="temp-1", value=21.5)
```

`durable()` journals the call to local SQLite before running it. If the
process crashes or the device loses power mid-call, the intent survives on
disk; the next `start()` (with `recovery=True`, the default) replays it
automatically. Arguments must be JSON-serializable and passed by name --
the decorated function can't declare `*args`/`**kwargs` -- and the function
must be safe to run more than once, since at-least-once execution means it
may run again for the same logical call.

## Testing

```bash
make test        # pytest
make check        # lint + typecheck + test
```

The test suite is deterministic: no real network access, real sleeps, or
physical hardware. Concurrency and crash-recovery behavior are tested with
in-process fakes (e.g. asserting 100 concurrent SQLite writes never lose a
row, or that a failed initialization never leaks an open database
connection).

## License

MIT -- see `LICENSE`.
