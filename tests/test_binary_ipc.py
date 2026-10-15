"""Comprehensive tests for ChronosMatch Binary IPC.

Covers:
- OrderCodec: 16-byte fixed layout, struct.Struct("!IIII"), price scaling, BUY/SELL, field validation
- BinaryRingBuffer: 16,392-byte file-backed mmap, 8-byte header, 1024 slots, 1023 usable orders
- Full/empty detection, wrap-around, strict index update ordering
- Context manager file lifecycle
- Multi-process producer-consumer execution using Windows spawn start method
"""

import multiprocessing
import os
import struct
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

import pytest

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
    MAX_UINT32,
    ORDER_SIZE,
    ORDER_STRUCT,
    TICKS_PER_RUPEE,
    OrderCodec,
    decode_order,
    encode_order,
)
from src.ipc.multiprocess_runner import run_multiprocess_simulation


class TestOrderCodec:
    """Tests for the 16-byte binary order codec."""

    def test_codec_constants(self) -> None:
        """Verify struct layout, size, and scale conform to specification."""
        assert ORDER_STRUCT.format == "!IIII"
        assert ORDER_SIZE == 16
        assert TICKS_PER_RUPEE == 100

    def test_encode_decode_buy_order(self) -> None:
        """Verify round-trip encoding and decoding of a BUY order."""
        order = {
            "order_id": 1001,
            "side": "BUY",
            "price": 150.25,
            "quantity": 10,
        }
        encoded = encode_order(order)
        assert len(encoded) == 16

        # Check raw big-endian fields: order_id, price_ticks, quantity, side
        order_id, ticks, qty, side_int = struct.unpack("!IIII", encoded)
        assert order_id == 1001
        assert ticks == 15025  # 150.25 * 100
        assert qty == 10
        assert side_int == 0  # 0 for BUY

        decoded = decode_order(encoded)
        assert decoded == order

    def test_encode_decode_sell_order(self) -> None:
        """Verify round-trip encoding and decoding of a SELL order."""
        order = {
            "order_id": 2048,
            "side": "SELL",
            "price": 99.00,
            "quantity": 50,
        }
        encoded = encode_order(order)
        order_id, ticks, qty, side_int = struct.unpack("!IIII", encoded)
        assert order_id == 2048
        assert ticks == 9900
        assert qty == 50
        assert side_int == 1  # 1 for SELL

        decoded = decode_order(encoded)
        assert decoded == order

    def test_integer_side_support(self) -> None:
        """Verify side can also be specified as 0 or 1."""
        order_buy = {"order_id": 1, "side": 0, "price": 10.0, "quantity": 1}
        order_sell = {"order_id": 2, "side": 1, "price": 20.0, "quantity": 1}

        dec_buy = decode_order(encode_order(order_buy))
        dec_sell = decode_order(encode_order(order_sell))

        assert dec_buy["side"] == "BUY"
        assert dec_sell["side"] == "SELL"

    def test_pack_into_and_unpack_from(self) -> None:
        """Verify direct packing into an existing bytearray or memoryview without allocation."""
        buf = bytearray(32)
        order1 = {"order_id": 10, "side": "BUY", "price": 55.50, "quantity": 5}
        order2 = {"order_id": 20, "side": "SELL", "price": 60.75, "quantity": 8}

        OrderCodec.pack_into(buf, 0, order1)
        OrderCodec.pack_into(buf, 16, order2)

        dec1 = OrderCodec.unpack_from(buf, 0)
        dec2 = OrderCodec.unpack_from(buf, 16)

        assert dec1 == order1
        assert dec2 == order2

    def test_validation_invalid_order_id(self) -> None:
        """Verify invalid order IDs are rejected."""
        # Negative
        with pytest.raises(ValueError, match="order_id"):
            encode_order({"order_id": -1, "side": "BUY", "price": 10.0, "quantity": 1})
        # Overflow uint32
        with pytest.raises(ValueError, match="order_id"):
            encode_order({"order_id": MAX_UINT32 + 1, "side": "BUY", "price": 10.0, "quantity": 1})
        # Boolean is rejected
        with pytest.raises(TypeError, match="order_id"):
            encode_order({"order_id": True, "side": "BUY", "price": 10.0, "quantity": 1})

    def test_validation_invalid_price(self) -> None:
        """Verify negative or non-numeric prices are rejected."""
        with pytest.raises(ValueError, match="price must be positive"):
            encode_order({"order_id": 1, "side": "BUY", "price": 0.0, "quantity": 1})
        with pytest.raises(ValueError, match="price must be positive"):
            encode_order({"order_id": 1, "side": "BUY", "price": -15.5, "quantity": 1})
        with pytest.raises(TypeError, match="price"):
            encode_order({"order_id": 1, "side": "BUY", "price": "100.0", "quantity": 1})

    def test_validation_invalid_quantity(self) -> None:
        """Verify invalid quantities are rejected."""
        with pytest.raises(ValueError, match="quantity"):
            encode_order({"order_id": 1, "side": "BUY", "price": 10.0, "quantity": 0})
        with pytest.raises(ValueError, match="quantity"):
            encode_order({"order_id": 1, "side": "BUY", "price": 10.0, "quantity": -5})
        with pytest.raises(TypeError, match="quantity"):
            encode_order({"order_id": 1, "side": "BUY", "price": 10.0, "quantity": 5.5})

    def test_validation_invalid_side(self) -> None:
        """Verify invalid side strings or numbers are rejected."""
        with pytest.raises(ValueError, match="Invalid side"):
            encode_order({"order_id": 1, "side": "HOLD", "price": 10.0, "quantity": 1})
        with pytest.raises(ValueError, match="Invalid side"):
            encode_order({"order_id": 1, "side": 2, "price": 10.0, "quantity": 1})

    def test_decode_short_buffer(self) -> None:
        """Verify decoding from a truncated buffer raises ValueError."""
        with pytest.raises(ValueError, match="Buffer too small"):
            decode_order(b"\x00" * 15)


class TestBinaryRingBuffer:
    """Tests for the file-backed memory-mapped binary ring buffer."""

    def test_buffer_geometry_and_file_size(self) -> None:
        """Verify the backing file is sized to exactly 16,392 bytes."""
        with BinaryRingBufferContext() as rb:
            assert rb.TOTAL_SLOTS == 1024
            assert rb.USABLE_SLOTS == 1023
            assert rb.TOTAL_MAPPING_SIZE == 16392
            assert rb.file_path is not None
            assert rb.file_path.stat().st_size == 16392

            read_idx, write_idx = rb.get_indices()
            assert read_idx == 0
            assert write_idx == 0
            assert rb.is_empty()
            assert not rb.is_full()
            assert rb.count() == 0

    def test_single_order_round_trip(self) -> None:
        """Verify putting and getting a single order through the mmap buffer."""
        order = {"order_id": 5001, "side": "BUY", "price": 250.75, "quantity": 15}
        with BinaryRingBufferContext() as rb:
            rb.put(order)
            assert rb.count() == 1
            assert not rb.is_empty()

            read_idx, write_idx = rb.get_indices()
            assert read_idx == 0
            assert write_idx == 1

            retrieved = rb.get()
            assert retrieved == order
            assert rb.is_empty()

            read_idx, write_idx = rb.get_indices()
            assert read_idx == 1
            assert write_idx == 1

    def test_empty_buffer_returns_none(self) -> None:
        """Verify get() on an empty buffer returns None."""
        with BinaryRingBufferContext() as rb:
            assert rb.get() is None
            assert rb.get_raw() is None

    def test_usable_capacity_and_buffer_full(self) -> None:
        """Verify exactly 1023 orders fill the buffer, and 1024th raises BufferError."""
        with BinaryRingBufferContext() as rb:
            for i in range(1023):
                rb.put({
                    "order_id": 1000 + i,
                    "side": "BUY" if i % 2 == 0 else "SELL",
                    "price": 10.0 + (i % 50),
                    "quantity": 1 + (i % 10),
                })

            assert rb.count() == 1023
            assert rb.is_full()

            # 1024th put must raise BufferError
            with pytest.raises(BufferError, match="Ring buffer is full"):
                rb.put({"order_id": 9999, "side": "BUY", "price": 100.0, "quantity": 5})

            # Read one order out to create a slot
            first = rb.get()
            assert first is not None
            assert first["order_id"] == 1000
            assert not rb.is_full()
            assert rb.count() == 1022

            # Now put should succeed
            rb.put({"order_id": 9999, "side": "BUY", "price": 100.0, "quantity": 5})
            assert rb.count() == 1023
            assert rb.is_full()

    def test_wrap_around_fifo_integrity(self) -> None:
        """Verify buffer wraps around slot 1023 back to 0 preserving FIFO order."""
        with BinaryRingBufferContext() as rb:
            # 1. Fill 600 orders
            for i in range(600):
                rb.put({"order_id": i, "side": "BUY", "price": 10.0, "quantity": 1})

            # 2. Drain 400 orders (read_idx advances to 400, write_idx is 600)
            for i in range(400):
                item = rb.get()
                assert item is not None
                assert item["order_id"] == i

            assert rb.count() == 200

            # 3. Put 700 orders -> wraps past 1024 back around to (600 + 700) % 1024 = 276
            for i in range(600, 1300):
                rb.put({"order_id": i, "side": "SELL", "price": 20.0, "quantity": 2})

            assert rb.count() == 900
            read_idx, write_idx = rb.get_indices()
            assert read_idx == 400
            assert write_idx == 276

            # 4. Drain all 900 orders and verify exact FIFO continuity
            for expected_id in range(400, 1300):
                item = rb.get()
                assert item is not None
                assert item["order_id"] == expected_id

            assert rb.is_empty()

    def test_open_existing_mapping(self) -> None:
        """Verify BinaryRingBuffer.open attaches to existing file without re-initializing."""
        fd, tmp_path = tempfile.mkstemp(suffix=".bin")
        os.close(fd)

        try:
            # 1. Parent creates and writes an order
            parent = BinaryRingBuffer.create(tmp_path)
            parent.put({"order_id": 42, "side": "BUY", "price": 100.0, "quantity": 1})
            parent.close()

            # 2. Child opens existing file
            child = BinaryRingBuffer.open(tmp_path)
            assert child.count() == 1
            retrieved = child.get()
            assert retrieved is not None
            assert retrieved["order_id"] == 42
            child.close()
        finally:
            if Path(tmp_path).exists():
                Path(tmp_path).unlink(missing_ok=True)


class TestCrossProcessIPC:
    """Tests executing real multi-process producer and consumer using Windows spawn."""

    def test_multiprocess_500_orders(self) -> None:
        """Verify cross-process delivery of 500 orders using spawn and shared lock."""
        prod, cons = run_multiprocess_simulation(
            total_orders=500,
            random_seed=42,
            timeout=15.0,
        )

        assert prod.get("status") == "success"
        assert cons.get("status") == "success"
        assert prod["orders_generated"] == 500
        assert prod["orders_submitted"] == 500
        assert prod["orders_failed"] == 0
        assert cons["orders_drained"] == 500
        assert cons["first_order_id"] == 1001
        assert cons["last_order_id"] == 1500
        assert prod["average_submission_latency_ns"] > 0
        assert prod["submitted_orders_per_second"] > 0

    def test_multiprocess_wrap_around_exceeding_capacity(self) -> None:
        """Verify cross-process streaming of 1500 orders (exceeds 1023 usable slots)."""
        prod, cons = run_multiprocess_simulation(
            total_orders=1500,
            random_seed=123,
            timeout=20.0,
        )

        assert prod.get("status") == "success"
        assert cons.get("status") == "success"
        assert prod["orders_submitted"] == 1500
        assert cons["orders_drained"] == 1500
        assert cons["first_order_id"] == 1001
        assert cons["last_order_id"] == 2500

    def test_multiprocess_clean_shutdown_and_cleanup(self) -> None:
        """Verify that the shared file is completely unlinked after both processes exit."""
        fd, custom_path = tempfile.mkstemp(prefix="chronos_test_cleanup_", suffix=".bin")
        os.close(fd)

        prod, cons = run_multiprocess_simulation(
            total_orders=100,
            file_path=custom_path,
            timeout=10.0,
        )

        assert prod["orders_submitted"] == 100
        assert cons["orders_drained"] == 100
        # Check that the backing file was safely unlinked
        assert not Path(custom_path).exists()

    def test_multiprocess_buffer_full_contention_and_retries(self) -> None:
        """Verify buffer-full retry behavior under actual producer-consumer contention.

        Producer generates 1,200 orders unthrottled, while consumer has a 0.05s startup delay.
        The buffer fills up to 1023 slots, forcing the producer into backpressure retries.
        Once the consumer begins draining, all retried orders succeed with 0 lost orders.
        """
        prod, cons = run_multiprocess_simulation(
            total_orders=1200,
            consumer_startup_delay=0.05,
            max_retries=15,
            retry_delay=0.005,
            random_seed=77,
            timeout=20.0,
        )

        assert prod.get("status") == "success"
        assert cons.get("status") == "success"
        assert prod["orders_submitted"] == 1200
        assert cons["orders_drained"] == 1200
        assert cons["first_order_id"] == 1001
        assert cons["last_order_id"] == 2200
        assert cons["fifo_violations"] == 0
        assert prod["buffer_full_events"] > 0, "Buffer full should trigger under contention"
        assert prod["retry_count"] > 0, "Retries should occur under contention"

