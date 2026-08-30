from __future__ import annotations

from pathlib import Path

from edgesentinel.metrics.checks import (
    cpu_load_ratio,
    memory_used_ratio,
    read_hardware_status,
    read_temperature,
)


def test_cpu_load_ratio_returns_a_non_negative_float() -> None:
    ratio = cpu_load_ratio()
    assert ratio >= 0.0


def test_memory_used_ratio_reads_a_real_meminfo_file(tmp_path: Path) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       1000 kB\nMemAvailable:    250 kB\n")

    ratio = memory_used_ratio(str(meminfo))

    assert ratio == 0.75


def test_memory_used_ratio_falls_back_to_memfree(tmp_path: Path) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       1000 kB\nMemFree:         400 kB\n")

    ratio = memory_used_ratio(str(meminfo))

    assert ratio == 0.6


def test_memory_used_ratio_is_zero_when_the_file_is_missing() -> None:
    assert memory_used_ratio("/no/such/meminfo") == 0.0


def test_memory_used_ratio_is_zero_when_mem_total_is_missing(tmp_path: Path) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemAvailable:    250 kB\n")

    assert memory_used_ratio(str(meminfo)) == 0.0


def test_memory_used_ratio_is_clamped_to_zero_and_one(tmp_path: Path) -> None:
    # MemAvailable reported larger than MemTotal shouldn't produce a
    # negative "used" ratio.
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       1000 kB\nMemAvailable:   2000 kB\n")

    assert memory_used_ratio(str(meminfo)) == 0.0


def test_read_temperature_reads_millidegrees_as_celsius(tmp_path: Path) -> None:
    thermal_zone = tmp_path / "temp"
    thermal_zone.write_text("45123\n")

    assert read_temperature(str(thermal_zone)) == 45.123


def test_read_temperature_is_none_when_the_path_is_missing() -> None:
    assert read_temperature("/no/such/thermal_zone") is None


def test_read_temperature_is_none_on_unparseable_content(tmp_path: Path) -> None:
    thermal_zone = tmp_path / "temp"
    thermal_zone.write_text("not-a-number\n")

    assert read_temperature(str(thermal_zone)) is None


def test_read_hardware_status_combines_all_three_readings(tmp_path: Path) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       1000 kB\nMemAvailable:    250 kB\n")
    thermal_zone = tmp_path / "temp"
    thermal_zone.write_text("50000\n")

    status = read_hardware_status(meminfo_path=str(meminfo), thermal_zone_path=str(thermal_zone))

    assert status.memory_used_ratio == 0.75
    assert status.temperature_celsius == 50.0
    assert status.cpu_load_ratio >= 0.0


def test_read_hardware_status_skips_temperature_when_path_is_none(tmp_path: Path) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       1000 kB\nMemAvailable:    250 kB\n")

    status = read_hardware_status(meminfo_path=str(meminfo), thermal_zone_path=None)

    assert status.temperature_celsius is None
