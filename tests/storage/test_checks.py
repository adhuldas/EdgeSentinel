from __future__ import annotations

from edgesentinel.storage.checks import StorageStatus, disk_usage


def test_disk_usage_reads_a_real_path(tmp_path: object) -> None:
    status = disk_usage(str(tmp_path))
    assert status.total_bytes > 0
    assert 0 <= status.free_bytes <= status.total_bytes


def test_free_byte_ratio() -> None:
    status = StorageStatus(free_bytes=25, total_bytes=100)
    assert status.free_byte_ratio == 0.25


def test_free_byte_ratio_is_zero_for_a_zero_size_filesystem() -> None:
    status = StorageStatus(free_bytes=0, total_bytes=0)
    assert status.free_byte_ratio == 0.0


def test_free_inode_ratio_is_none_when_inode_counts_are_unavailable() -> None:
    status = StorageStatus(free_bytes=1, total_bytes=1)
    assert status.free_inode_ratio is None


def test_free_inode_ratio() -> None:
    status = StorageStatus(free_bytes=1, total_bytes=1, free_inodes=10, total_inodes=40)
    assert status.free_inode_ratio == 0.25
