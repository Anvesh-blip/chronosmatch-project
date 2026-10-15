"""ChronosMatch File-Backed Memory-Mapped Binary Ring Buffer.

Implementation of the agreed 16-byte binary IPC ring buffer:
- Header: 8 bytes (two 32-bit big-endian unsigned integers: read_index, write_index)
- Total slots: 1024
- Usable orders: 1023 (circular buffer with single-slot empty/full distinction)
- Slot size: 16 bytes (using struct.Struct("!IIII"))
- Total mapping size: 8 + (1024 * 16) = 16392 bytes
- Thread/Process safety: synchronizes read/write indices and record slots via a shared multiprocessing.Lock.
- Strict ordering: Producer writes complete record before publishing write_index;
                   Consumer reads complete record before publishing read_index.
"""

from __future__ import annotations

import logging
import mmap
import os
import struct
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from src.ipc.codec import ORDER_SIZE, OrderCodec

logger = logging.getLogger("binary_ring_buffer")

# Constants
HEADER_STRUCT = struct.Struct("!II")  # read_index, write_index
HEADER_SIZE = HEADER_STRUCT.size      # 8 bytes
TOTAL_SLOTS = 1024
USABLE_SLOTS = TOTAL_SLOTS - 1        # 1023
TOTAL_MAPPING_SIZE = HEADER_SIZE + (TOTAL_SLOTS * ORDER_SIZE)  # 16392 bytes


class BinaryRingBuffer:
    """Zero-copy / low-overhead memory-mapped ring buffer for binary orders."""

    HEADER_SIZE: int = HEADER_SIZE
    TOTAL_SLOTS: int = TOTAL_SLOTS
    USABLE_SLOTS: int = USABLE_SLOTS
    ORDER_SIZE: int = ORDER_SIZE
    TOTAL_MAPPING_SIZE: int = TOTAL_MAPPING_SIZE

    def __init__(
        self,
        mmap_obj: mmap.mmap,
        lock: Optional[Any] = None,
        file_obj: Optional[Any] = None,
        file_path: Optional[Union[str, Path]] = None,
    ) -> None:
        """Initialize the BinaryRingBuffer instance.

        Users typically call BinaryRingBuffer.create(...) or BinaryRingBuffer.open(...).
        """
        self._mmap = mmap_obj
        self._lock = lock
        self._file_obj = file_obj
        self._file_path = Path(file_path) if file_path else None
        self._closed = False

    @property
    def capacity(self) -> int:
        """Total number of slots allocated in the ring buffer."""
        return self.TOTAL_SLOTS

    @property
    def usable_capacity(self) -> int:
        """Maximum number of orders the buffer can hold before raising BufferError."""
        return self.USABLE_SLOTS

    @property
    def file_path(self) -> Optional[Path]:
        """Path to the backing file, if applicable."""
        return self._file_path

    @classmethod
    def create(
        cls,
        file_path: Union[str, Path],
        lock: Optional[Any] = None,
    ) -> BinaryRingBuffer:
        """Create and initialize a new file-backed mmap ring buffer.

        Called exclusively by the parent process. Sizes the file to exactly
        16,392 bytes and initializes the 8-byte header (read_index=0, write_index=0).
        """
        path = Path(file_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Open in read-write-create mode
        f = open(path, "w+b")
        try:
            # Resize file to exact mapping size
            f.truncate(TOTAL_MAPPING_SIZE)
            f.seek(0)
            # Pre-fill with zeros
            f.write(b"\x00" * TOTAL_MAPPING_SIZE)
            f.flush()

            # Create memory mapping
            # On Windows, fileno() is used, length=TOTAL_MAPPING_SIZE
            mm = mmap.mmap(f.fileno(), TOTAL_MAPPING_SIZE, access=mmap.ACCESS_WRITE)

            # Explicitly write initial header: read_index = 0, write_index = 0
            HEADER_STRUCT.pack_into(mm, 0, 0, 0)
            mm.flush()

            return cls(mmap_obj=mm, lock=lock, file_obj=f, file_path=path)
        except Exception:
            f.close()
            raise

    @classmethod
    def open(
        cls,
        file_path: Union[str, Path],
        lock: Optional[Any] = None,
    ) -> BinaryRingBuffer:
        """Attach to an existing file-backed mmap ring buffer.

        Called by child (spawned) producer or consumer processes. Does NOT
        re-initialize the header or overwrite existing data.
        """
        path = Path(file_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Backing file for ring buffer does not exist: {path}")

        file_size = path.stat().st_size
        if file_size < TOTAL_MAPPING_SIZE:
            raise ValueError(
                f"Backing file size ({file_size} bytes) is smaller than required mapping size "
                f"({TOTAL_MAPPING_SIZE} bytes)"
            )

        f = open(path, "r+b")
        try:
            mm = mmap.mmap(f.fileno(), TOTAL_MAPPING_SIZE, access=mmap.ACCESS_WRITE)
            return cls(mmap_obj=mm, lock=lock, file_obj=f, file_path=path)
        except Exception:
            f.close()
            raise

    def _get_indices(self) -> Tuple[int, int]:
        """Read (read_index, write_index) from the 8-byte shared header."""
        return HEADER_STRUCT.unpack_from(self._mmap, 0)

    def _set_write_index(self, new_write: int) -> None:
        """Update write_index at offset 4 of the shared header."""
        struct.pack_into("!I", self._mmap, 4, new_write)

    def _set_read_index(self, new_read: int) -> None:
        """Update read_index at offset 0 of the shared header."""
        struct.pack_into("!I", self._mmap, 0, new_read)

    @staticmethod
    def _slot_offset(slot_idx: int) -> int:
        """Calculate byte offset for slot index."""
        return HEADER_SIZE + (slot_idx * ORDER_SIZE)

    def put(self, order: Union[Dict[str, Any], bytes]) -> None:
        """Insert an order into the ring buffer.

        Accepts either an order dictionary (automatically packed) or raw 16 bytes.

        Raises:
            BufferError: If the ring buffer is full.
            ValueError/TypeError: If order dictionary validation fails.
        """
        if self._closed:
            raise RuntimeError("Cannot put to closed BinaryRingBuffer")

        # Encode before acquiring lock if it's a dict to minimize lock-hold duration
        if isinstance(order, dict):
            packed_bytes = OrderCodec.encode(order)
        elif isinstance(order, (bytes, bytearray, memoryview)):
            if len(order) != ORDER_SIZE:
                raise ValueError(f"Raw order must be exactly {ORDER_SIZE} bytes, got {len(order)}")
            packed_bytes = order
        else:
            raise TypeError(f"Order must be a dict or {ORDER_SIZE}-byte bytes object, got {type(order).__name__}")

        if self._lock:
            with self._lock:
                self._put_internal(packed_bytes)
        else:
            self._put_internal(packed_bytes)

    def _put_internal(self, packed_bytes: bytes) -> None:
        """Internal put logic executed under lock."""
        read_idx, write_idx = self._get_indices()
        next_write = (write_idx + 1) % self.TOTAL_SLOTS

        if next_write == read_idx:
            raise BufferError("Ring buffer is full")

        offset = self._slot_offset(write_idx)

        # 1. Write the complete 16-byte record FIRST
        self._mmap[offset : offset + ORDER_SIZE] = packed_bytes

        # 2. Publish updated write_index in the shared header
        self._set_write_index(next_write)

    def get(self) -> Optional[Dict[str, Any]]:
        """Retrieve and decode the next order from the ring buffer.

        Returns:
            Decoded order dictionary, or None if the buffer is empty.
        """
        raw = self.get_raw()
        if raw is None:
            return None
        return OrderCodec.decode(raw)

    def get_raw(self) -> Optional[bytes]:
        """Retrieve the raw 16 bytes of the next order from the ring buffer.

        Returns:
            16 bytes of packed order, or None if empty.
        """
        if self._closed:
            raise RuntimeError("Cannot get from closed BinaryRingBuffer")

        if self._lock:
            with self._lock:
                return self._get_raw_internal()
        else:
            return self._get_raw_internal()

    def _get_raw_internal(self) -> Optional[bytes]:
        """Internal get logic executed under lock."""
        read_idx, write_idx = self._get_indices()

        if read_idx == write_idx:
            return None

        offset = self._slot_offset(read_idx)

        # 1. Read the complete 16-byte record FIRST
        record = bytes(self._mmap[offset : offset + ORDER_SIZE])

        # 2. Advance and publish read_index in the shared header
        next_read = (read_idx + 1) % self.TOTAL_SLOTS
        self._set_read_index(next_read)

        return record

    def count(self) -> int:
        """Return the current number of orders stored in the buffer."""
        if self._closed:
            return 0
        if self._lock:
            with self._lock:
                r, w = self._get_indices()
        else:
            r, w = self._get_indices()
        return (w - r) % self.TOTAL_SLOTS

    def is_empty(self) -> bool:
        """Check whether the buffer has 0 unread orders."""
        return self.count() == 0

    def is_full(self) -> bool:
        """Check whether the buffer is at maximum capacity (1023 orders)."""
        if self._closed:
            return False
        if self._lock:
            with self._lock:
                r, w = self._get_indices()
        else:
            r, w = self._get_indices()
        return ((w + 1) % self.TOTAL_SLOTS) == r

    def get_indices(self) -> Tuple[int, int]:
        """Return (read_index, write_index)."""
        if self._lock:
            with self._lock:
                return self._get_indices()
        return self._get_indices()

    def close(self) -> None:
        """Flush memory mapping and close file handles."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._mmap is not None and not self._mmap.closed:
                self._mmap.flush()
                self._mmap.close()
        except Exception as e:
            logger.debug("Error closing mmap: %s", e)

        try:
            if self._file_obj is not None and not self._file_obj.closed:
                self._file_obj.close()
        except Exception as e:
            logger.debug("Error closing file: %s", e)

    def __enter__(self) -> BinaryRingBuffer:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class BinaryRingBufferContext:
    """Context manager for the lifecycle of a file-backed binary ring buffer.

    Creates the temporary file, initializes the parent BinaryRingBuffer,
    and safely removes the file after all processes have terminated and closed.
    """

    def __init__(
        self,
        file_path: Optional[Union[str, Path]] = None,
        lock: Optional[Any] = None,
        delete_on_close: bool = True,
    ) -> None:
        if file_path is None:
            fd, path = tempfile.mkstemp(prefix="chronos_ring_", suffix=".bin")
            os.close(fd)
            self.file_path = Path(path)
        else:
            self.file_path = Path(file_path).resolve()

        self.lock = lock
        self.delete_on_close = delete_on_close
        self.buffer: Optional[BinaryRingBuffer] = None

    def __enter__(self) -> BinaryRingBuffer:
        self.buffer = BinaryRingBuffer.create(self.file_path, lock=self.lock)
        return self.buffer

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.buffer is not None:
            self.buffer.close()

        if self.delete_on_close and self.file_path.exists():
            # Retry deletion up to 5 times to accommodate Windows file release timing
            for _ in range(5):
                try:
                    self.file_path.unlink(missing_ok=True)
                    break
                except PermissionError:
                    time.sleep(0.05)
