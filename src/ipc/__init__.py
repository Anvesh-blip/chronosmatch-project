"""IPC module for ChronosMatch.

Provides both the prototype in-memory RingBuffer (Anvesh's initial prototype)
and the production-grade file-backed BinaryRingBuffer (mmap + struct IPC)
with 16-byte fixed-layout binary order encoding.
"""

from src.ipc.binary_ring_buffer import (
    BinaryRingBuffer,
    BinaryRingBufferContext,
    HEADER_SIZE,
    HEADER_STRUCT,
    TOTAL_MAPPING_SIZE,
    TOTAL_SLOTS,
    USABLE_SLOTS,
)
from src.ipc.codec import (
    ORDER_SIZE,
    ORDER_STRUCT,
    TICKS_PER_RUPEE,
    OrderCodec,
    decode_order,
    encode_order,
)
from src.ipc.ring_buffer import RingBuffer

__all__ = [
    "RingBuffer",
    "BinaryRingBuffer",
    "BinaryRingBufferContext",
    "OrderCodec",
    "encode_order",
    "decode_order",
    "HEADER_STRUCT",
    "HEADER_SIZE",
    "ORDER_STRUCT",
    "ORDER_SIZE",
    "TOTAL_SLOTS",
    "USABLE_SLOTS",
    "TOTAL_MAPPING_SIZE",
    "TICKS_PER_RUPEE",
]
