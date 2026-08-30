"""Filesystem usage primitives used by :class:`~edgeguard.storage.monitor.StorageMonitor`.

Kept separate from the monitor itself for the same reason as
:mod:`edgeguard.network.checks`: the monitor should have no opinion about
*how* usage is measured, so it can be driven entirely by fakes in tests.
Both ``shutil.disk_usage`` and ``os.statvfs`` are plain, fast, local
syscalls -- unlike the network checks, there's no need for these to be
async.
"""

from __future__ import annotations

import dataclasses
import os
import shutil


@dataclasses.dataclass(frozen=True, slots=True)
class StorageStatus:
    """A snapshot of free space and free inodes for one filesystem path."""

    free_bytes: int
    total_bytes: int
    free_inodes: int | None = None
    total_inodes: int | None = None

    @property
    def free_byte_ratio(self) -> float:
        return self.free_bytes / self.total_bytes if self.total_bytes else 0.0

    @property
    def free_inode_ratio(self) -> float | None:
        """``None`` if the filesystem doesn't report inode counts at all."""
        if self.total_inodes is None or self.free_inodes is None or self.total_inodes == 0:
            return None
        return self.free_inodes / self.total_inodes


def disk_usage(path: str) -> StorageStatus:
    """Read real free space and inode counts for ``path``.

    ``free_inodes``/``total_inodes`` are ``None`` wherever ``os.statvfs``
    isn't available (i.e. off Linux/POSIX) rather than raising, since inode
    exhaustion is a secondary signal to free bytes, not the primary one --
    a caller that only cares about bytes shouldn't have to special-case the
    platform.
    """
    usage = shutil.disk_usage(path)
    statvfs = getattr(os, "statvfs", None)
    if statvfs is None:
        return StorageStatus(free_bytes=usage.free, total_bytes=usage.total)
    stats = statvfs(path)
    return StorageStatus(
        free_bytes=usage.free,
        total_bytes=usage.total,
        free_inodes=stats.f_favail,
        total_inodes=stats.f_files,
    )
