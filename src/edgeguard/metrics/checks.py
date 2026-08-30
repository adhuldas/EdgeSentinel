"""Stdlib-only hardware metrics primitives used by
:class:`~edgeguard.metrics.monitor.MetricsMonitor`.

Kept separate from the monitor itself for the same reason as
:mod:`edgeguard.storage.checks`: the monitor should have no opinion about
*how* usage is measured, so it can be driven entirely by fakes in tests.
Every read here is a plain, fast, local syscall or file read -- no
``psutil``, no external dependency -- consistent with edgeguard shipping
zero runtime dependencies by design.

Everything here is Linux-specific (``/proc``, ``/sys``, ``os.getloadavg``).
Rather than raising off Linux -- or on hardware/containers missing a given
source -- each function returns a value meaning "nothing to report" (``0.0``
or ``None``), so a caller that only cares about one metric never has to
special-case the platform for the others.
"""

from __future__ import annotations

import dataclasses
import os


@dataclasses.dataclass(frozen=True, slots=True)
class HardwareStatus:
    """A snapshot of CPU load, memory usage, and (optionally) temperature."""

    cpu_load_ratio: float
    memory_used_ratio: float
    temperature_celsius: float | None = None


def cpu_load_ratio() -> float:
    """The 1-minute load average divided by CPU count.

    ``1.0`` means "as busy as there are cores to handle"; it can exceed
    ``1.0`` under contention, the same way raw load averages can exceed the
    core count. Returns ``0.0`` wherever ``os.getloadavg()`` isn't
    available (notably Windows) rather than raising.
    """
    getloadavg = getattr(os, "getloadavg", None)
    if getloadavg is None:
        return 0.0
    # `getattr` with a dynamic default erases the return type to `Any` even
    # though `os.getloadavg` itself is fully typed -- assigning through an
    # explicitly-annotated local accepts the implicit narrowing back to
    # `float`, the same pattern used for `paho.Client()` in
    # edgeguard.integrations.mqtt.
    load_1min: float = getloadavg()[0]
    cpu_count = os.cpu_count() or 1
    return load_1min / cpu_count


def memory_used_ratio(meminfo_path: str = "/proc/meminfo") -> float:
    """The fraction of physical memory currently in use, from ``/proc/meminfo``.

    Uses ``MemAvailable`` -- which accounts for reclaimable page cache and
    buffers, unlike naively treating all non-free memory as "used" -- and
    falls back to ``MemFree`` on kernels too old to report it. Returns
    ``0.0`` wherever the file can't be read or parsed (i.e. off Linux)
    rather than raising.
    """
    try:
        with open(meminfo_path) as f:
            lines = f.readlines()
    except OSError:
        return 0.0
    values: dict[str, int] = {}
    for line in lines:
        key, _sep, rest = line.partition(":")
        digits = "".join(char for char in rest if char.isdigit())
        if digits:
            values[key.strip()] = int(digits)
    total = values.get("MemTotal")
    if not total:
        return 0.0
    available = values.get("MemAvailable", values.get("MemFree"))
    if available is None:
        return 0.0
    return max(0.0, min(1.0, 1 - (available / total)))


def read_temperature(
    thermal_zone_path: str = "/sys/class/thermal/thermal_zone0/temp",
) -> float | None:
    """The CPU/SoC temperature in Celsius, from a Linux thermal zone.

    Returns ``None`` wherever the path doesn't exist or can't be parsed --
    off Linux, on hardware without a thermal zone, or in a container
    without ``/sys`` mounted -- rather than raising, since temperature is
    the metric least likely to be available on any given device.
    """
    try:
        with open(thermal_zone_path) as f:
            millidegrees = int(f.read().strip())
    except (OSError, ValueError):
        return None
    return millidegrees / 1000.0


def read_hardware_status(
    *,
    meminfo_path: str = "/proc/meminfo",
    thermal_zone_path: str | None = "/sys/class/thermal/thermal_zone0/temp",
) -> HardwareStatus:
    """Read CPU load, memory usage, and (if available) temperature in one call.

    Args:
        meminfo_path: Passed to :func:`memory_used_ratio`.
        thermal_zone_path: Passed to :func:`read_temperature`. ``None``
            skips reading temperature entirely (leaving it ``None`` on the
            returned status) rather than trying a default path that isn't
            expected to exist on this device.
    """
    return HardwareStatus(
        cpu_load_ratio=cpu_load_ratio(),
        memory_used_ratio=memory_used_ratio(meminfo_path),
        temperature_celsius=(
            read_temperature(thermal_zone_path) if thermal_zone_path is not None else None
        ),
    )
