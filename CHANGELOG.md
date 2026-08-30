# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

edgesentinel is being built in phases (see `README.md#development-status`).
This is **Phase 7**: the runtime is not yet feature-complete and should not
be used in production.

### Added

- Hardware metrics monitoring (`edgesentinel.metrics.MetricsMonitor`): polls
  CPU load average, memory pressure, and (where available) SoC
  temperature, stdlib-only (`os.getloadavg`, `/proc/meminfo`, a Linux
  thermal zone under `/sys/class/thermal`) via injectable checks (see
  `edgesentinel.metrics.checks`) so tests never touch the real filesystem or
  depend on running on Linux. Every reading gracefully reports "nothing to
  report" (`0.0`/`None`) rather than raising wherever its source isn't
  available, so a caller that only cares about one metric never has to
  special-case the platform for the others.
- Mirrors `StorageMonitor`'s low/high-water-mark shape for "too much"
  instead of "too little": any of `cpu_high`, `memory_high`, or
  `temperature_high_celsius` being crossed runs a caller-supplied sequence
  of mitigation actions, in order, re-checking every metric after each and
  stopping once nothing is high any more. Exhausting configured
  mitigations without recovering escalates the runtime, same as storage
  cleanup exhaustion; a monitor with no mitigations configured is a plain
  DEGRADED observer instead.
- `guard.watch_hardware()`: factory method wiring a `MetricsMonitor` to the
  runtime's event bus and lifecycle state (`HEALTHY`/`DEGRADED`/`FAILED`),
  the same registration-before-start contract as `guard.watch_network()`
  and `guard.watch_storage()`.
- HTTP webhook forwarding (`edgesentinel.integrations.HttpEventPublisher`):
  subscribes to an `EventBus` and POSTs each event as JSON to a configured
  URL, built on stdlib `urllib.request` wrapped in `asyncio.to_thread` --
  no extra dependency needed. Supports extra headers, a per-request
  timeout, and `min_severity` filtering; a transport failure or non-2xx
  status is logged, never propagated, the same way `EventBus.publish`
  itself never lets a subscriber's exception escape.
- MQTT forwarding (`edgesentinel.integrations.MqttPublisher`): the same
  attach/detach/`min_severity` shape as the HTTP publisher, but talking to
  a small structural `MqttClient` protocol instead of a concrete library,
  so the module has no import-time dependency on `paho-mqtt`. Connecting
  for real (`connect()`, with no client supplied) lazily imports
  `paho.mqtt.client` and raises a clear `ImportError` with install
  instructions if the optional `edgesentinel[mqtt]` extra isn't installed.
  Topics are either a fixed string or a per-event `TopicBuilder` callable,
  defaulting to `edgesentinel/{component}/{type}`.
- Durable event timeline (`edgesentinel.diagnostics.EventLog`): subscribes to
  the runtime's `EventBus` and persists every published event to a new
  `events` table (migration version 4), so "what happened, and when"
  survives a restart and is inspectable from a separate process (e.g. the
  CLI). `query()` filters by component, type, minimum severity, and a
  timestamp range, newest- or oldest-first; `prune_older_than()` bounds the
  table's growth on long-running devices. Events are ordered by insertion
  (`id`), not `timestamp`, since SQLite's one-second timestamp resolution
  can't disambiguate several events published in the same second.
- Incident tracking (`edgesentinel.diagnostics.IncidentTracker`,
  `build_incidents()`): every Phase 4 subsystem escalates through the same
  `RuntimeState` lifecycle rather than reporting problems directly, so a
  single `state_change` event stream is enough to derive "incidents" --
  spans of non-`HEALTHY` time -- without per-subsystem pairing logic.
  `IncidentTracker` does this live, attached to a running runtime's
  `EventBus`, bounded by `max_history`; `build_incidents()` reconstructs
  the same incidents offline by replaying an already-persisted,
  chronologically-ordered event sequence -- what the CLI uses, since it has
  no live bus to attach to.
- Human-readable rendering (`edgesentinel.diagnostics.report`): `format_event`,
  `format_timeline`, `format_incident`, `format_incidents`, and
  `summarize_incidents`, shared by the CLI and any future dashboard so
  formatting logic isn't duplicated between them.
- `guard.timeline` and `guard.incidents`: an `EventLog` and `IncidentTracker`
  wired to every `EdgeSentinel` instance, attached right after migrations run
  during `start()` (so even the BOOTING/INITIALIZING boot sequence is
  captured) and detached before the database closes during `stop()`.
- The `edgesentinel` CLI (`edgesentinel.cli`, installed as a console script):
  `edgesentinel --name <name> --data-dir <dir> status|timeline|incidents`.
  Reads a runtime's on-disk SQLite database directly via a fresh
  `Database`/`EventLog` pair -- it never talks to a running process, so it
  works the same whether the runtime is live or stopped, thanks to WAL
  mode letting a separate reader never block a writer. `timeline` supports
  `--component`, `--type`, `--min-severity`, `--limit`, and
  `--oldest-first`; `incidents` replays the timeline through
  `build_incidents()` and prints a summary.
- Layered connectivity monitoring (`edgesentinel.network`): `tcp_reachable()` /
  `dns_resolves()` stdlib-only checks, and `NetworkMonitor`, which polls a
  configurable subset of LINK/GATEWAY/DNS/INTERNET checks bottom-up,
  stopping at the first failing layer, and publishes `network_status_changed`
  only when the highest reachable layer actually changes.
- In-process task supervision (`edgesentinel.process`): `Supervisor` restarts a
  crashed or exited async task with backoff, and gives up -- reporting
  itself `crashed` -- after `max_crashes` failures within a sliding time
  window, rather than restarting a broken task forever.
- Heartbeat-based staleness detection (`edgesentinel.process.Watchdog`):
  registered targets call `heartbeat()` from their own loop; anything that
  stops checking in within its configured timeout is reported stale via
  `watchdog_target_stale` / `watchdog_target_recovered` events, published
  only on the transition, not on every poll.
- Free-space and inode monitoring with scoped cleanup
  (`edgesentinel.storage`): `StorageMonitor` polls free bytes (and, optionally,
  free inodes) on a path and runs a caller-supplied sequence of cleanup
  actions, in order, when usage drops below a low-water mark, stopping as
  soon as one frees enough space.
- Both `NetworkMonitor` and `StorageMonitor` can drive the runtime's
  lifecycle state (`HEALTHY` / `DEGRADED` / `OFFLINE`) as connectivity or
  storage health changes; `Supervisor` and `Watchdog` can escalate the
  runtime to `FAILED` when they give up. All four only touch runtime states
  they own, so concurrent subsystems can't fight each other or interfere
  with boot/shutdown.
- `EdgeSentinel.watch_network()`, `EdgeSentinel.supervise()`, `EdgeSentinel.watchdog`
  (a lazily-created, shared `Watchdog`), and `EdgeSentinel.watch_storage()`
  factory methods that auto-wire a new subsystem to the runtime's event bus
  and lifecycle state. Like `guard.durable(...)`, registering a subsystem
  before `start()` also makes the runtime start and stop it automatically
  alongside its own lifecycle.
- Project scaffolding: `pyproject.toml` (hatchling, src layout), Ruff, MyPy
  (strict), pytest/pytest-asyncio, pre-commit, GitHub Actions CI.
- `EdgeSentinel` runtime with `start()` / `stop()` / async context manager.
- Strongly-typed lifecycle state machine (`RuntimeState`) with an explicit,
  validated transition graph. Invalid transitions raise
  `InvalidStateTransitionError`.
- Async event bus (`EventBus`, `Event`) and the `on_state_change` decorator
  for observing lifecycle transitions (`StateChangeEvent`).
- Local SQLite persistence layer (`Database`) built on stdlib `sqlite3` +
  `asyncio.to_thread`: WAL mode, versioned migrations, transactions.
- Runtime state (name + current lifecycle state) is persisted on start and
  stop, laying the groundwork for crash/reboot recovery in Phase 3.
- Backoff algorithms (`fixed`, `linear`, `exponential`) with optional full
  jitter (`compute_delay`).
- `RetryPolicy`: configurable max attempts, backoff, exception filtering, and
  an `on_retry` hook, with no real sleeping needed in tests.
- Per-attempt timeouts (`with_timeout`, `OperationTimeoutError`).
- `CircuitBreaker`: CLOSED/OPEN/HALF_OPEN state machine with concurrency-safe
  single in-flight HALF_OPEN trial, optional persistence via a structural
  `CircuitBreakerStore` protocol, and wall-clock reconciliation on
  `restore()` so an open circuit survives a process restart.
- `guard.reliable()` decorator composing circuit breaker (outermost),
  retry/backoff (middle), and per-attempt timeout (innermost) around an
  async function, publishing `retry_attempt`, `operation_succeeded`, and
  `operation_failed` events onto the runtime's event bus.
- Write-ahead intent journal (`intents` table, `IntentJournal`): every
  durable call is recorded `pending` before it runs and moves through
  `in_progress` -> `completed`/`failed` (or back to `pending` for another
  attempt), keyed by a `rowid`-ordered, crash-safe insertion order rather
  than a low-resolution timestamp column.
- `guard.durable(operation)` decorator giving an async function
  at-least-once, crash/reboot-safe execution semantics: arguments are
  bound by name and journaled as JSON before the function runs, so the
  same call can be replayed verbatim after a crash. Registration works
  before `start()`; calling the decorated function requires the runtime to
  be started and raises `RuntimeNotStartedError` otherwise, since it must
  write to the journal first. Publishes `durable_operation_started`,
  `durable_operation_completed`, `durable_operation_retry_pending`, and
  `durable_operation_exhausted` events.
- Startup replay (`replay_pending`, wired into `start()`, disabled via
  `EdgeSentinel(..., recovery=False)`): every intent left `pending` or
  `in_progress` by a previous run is replayed before the runtime becomes
  healthy. Each intent replays independently -- one intent failing, or an
  intent whose operation isn't registered on this boot, is logged and
  reported via a `durable_operation_unhandled` event but never blocks the
  rest of the journal or the runtime from starting.
- `IntentJournal.prune_completed()` / `Database.prune_completed_intents()`
  to bound the journal's growth on long-running devices.
- `InvalidDurablePayloadError` (also a `TypeError`) for non-JSON-serializable
  payloads or functions declaring `*args`/`**kwargs`, and
  `DurableOperationExhaustedError` when a durable operation's `max_attempts`
  is exhausted.

## [0.1.0] - Unreleased

Initial scaffolding release. Not yet published to PyPI.
