"""Tests for ChronosMatch Market Simulator (Arjit's Module).

Covers:
- Order schema, fields, and data types
- BUY/SELL sides and positive price/quantity bounds
- Unique order IDs
- Reproducibility with random seeds
- Successful submissions to RingBuffer
- Buffer-full retry handling and backpressure
- Retry limit handling (continue vs stop)
- Unexpected submission exceptions and statistics accuracy
- Configurable order counts and generation rate pacing
- Binary encoder extensibility
- Simulator cancellation
"""

import asyncio
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from src.ipc.ring_buffer import RingBuffer
from src.simulator.market_simulator import (
    MarketSimulator,
    OrderGenerator,
    SimulationMetrics,
    SimulatorConfig,
)


class TestOrderGenerator:
    """Tests for the synthetic order generation logic."""

    def test_order_fields_and_data_types(self) -> None:
        """Verify order dictionary contains exact required fields and types."""
        gen = OrderGenerator(start_order_id=1001, seed=42)
        order = gen.generate_order()

        assert isinstance(order, dict)
        assert set(order.keys()) == {"order_id", "side", "price", "quantity"}

        assert isinstance(order["order_id"], int)
        assert isinstance(order["side"], str)
        assert isinstance(order["price"], (int, float))
        assert isinstance(order["quantity"], int)

    def test_valid_sides(self) -> None:
        """Verify sides are strictly BUY or SELL and both are produced."""
        gen = OrderGenerator(seed=123)
        sides = {gen.generate_order()["side"] for _ in range(100)}
        assert sides == {"BUY", "SELL"}

    def test_positive_prices_and_quantities(self) -> None:
        """Verify all generated prices and quantities are strictly positive."""
        gen = OrderGenerator(min_price=1.0, max_price=50.0, min_quantity=1, max_quantity=20, seed=99)
        for _ in range(200):
            order = gen.generate_order()
            assert order["price"] > 0, f"Price must be positive, got {order['price']}"
            assert order["quantity"] >= 1, f"Quantity must be >= 1, got {order['quantity']}"

    def test_unique_sequential_order_ids(self) -> None:
        """Verify order IDs are unique and increment sequentially."""
        start_id = 5000
        count = 1000
        gen = OrderGenerator(start_order_id=start_id, seed=77)
        order_ids = [gen.generate_order()["order_id"] for _ in range(count)]

        assert len(order_ids) == len(set(order_ids)), "Order IDs must be unique"
        assert order_ids[0] == start_id
        assert order_ids[-1] == start_id + count - 1
        assert order_ids == list(range(start_id, start_id + count))

    def test_reproducibility_with_fixed_seed(self) -> None:
        """Verify that identical seeds produce identical order streams."""
        gen1 = OrderGenerator(start_order_id=1001, seed=42)
        orders1 = [gen1.generate_order() for _ in range(50)]

        gen2 = OrderGenerator(start_order_id=1001, seed=42)
        orders2 = [gen2.generate_order() for _ in range(50)]

        assert orders1 == orders2

        gen3 = OrderGenerator(start_order_id=1001, seed=999)
        orders3 = [gen3.generate_order() for _ in range(50)]
        assert orders1 != orders3


class TestSimulatorConfig:
    """Tests for simulator configuration validation."""

    def test_valid_config(self) -> None:
        cfg = SimulatorConfig(total_orders=500, orders_per_second=100.0, max_retries=3)
        assert cfg.total_orders == 500
        assert cfg.orders_per_second == 100.0
        assert cfg.max_retries == 3

    def test_invalid_total_orders(self) -> None:
        with pytest.raises(ValueError, match="total_orders"):
            SimulatorConfig(total_orders=-1)

    def test_invalid_rate(self) -> None:
        with pytest.raises(ValueError, match="orders_per_second"):
            SimulatorConfig(orders_per_second=-10.0)

    def test_invalid_retry_delay(self) -> None:
        with pytest.raises(ValueError, match="retry_delay"):
            SimulatorConfig(retry_delay=-0.5)

    def test_invalid_on_exhausted_policy(self) -> None:
        with pytest.raises(ValueError, match="on_retry_exhausted"):
            SimulatorConfig(on_retry_exhausted="drop")

    def test_invalid_price_range(self) -> None:
        with pytest.raises(ValueError, match="min_price must be positive"):
            SimulatorConfig(min_price=-5.0)
        with pytest.raises(ValueError, match="max_price"):
            SimulatorConfig(min_price=100.0, max_price=50.0)


class TestMarketSimulatorExecution:
    """Tests for asynchronous submission and IPC coordination."""

    @pytest.mark.asyncio
    async def test_successful_submissions(self) -> None:
        """Test standard run where RingBuffer has ample capacity."""
        order_count = 50
        rb = RingBuffer(capacity=order_count + 10)
        config = SimulatorConfig(total_orders=order_count, random_seed=42)

        simulator = MarketSimulator(ring_buffer=rb, config=config)
        metrics = await simulator.run()

        assert metrics.orders_generated == order_count
        assert metrics.orders_submitted == order_count
        assert metrics.orders_failed == 0
        assert metrics.buffer_full_events == 0
        assert metrics.retry_count == 0
        assert metrics.total_execution_time_seconds > 0
        assert metrics.submitted_orders_per_second > 0
        assert metrics.average_submission_latency_ns > 0

        # Verify ring buffer received exact orders in FIFO order
        for expected_id in range(1001, 1001 + order_count):
            item = rb.get()
            assert item is not None
            assert item["order_id"] == expected_id

    @pytest.mark.asyncio
    async def test_buffer_full_retry_with_consumer_drain(self) -> None:
        """Test buffer full condition resolved by concurrent consumer draining."""
        # Buffer capacity 5 can hold 4 orders
        rb = RingBuffer(capacity=5)
        config = SimulatorConfig(
            total_orders=12,
            max_retries=10,
            retry_delay=0.005,
            random_seed=123,
        )

        simulator = MarketSimulator(ring_buffer=rb, config=config)

        # Background consumer that starts draining after a brief pause
        async def delayed_consumer() -> None:
            await asyncio.sleep(0.01)  # Allow buffer to fill first
            while True:
                item = rb.get()
                if item is None:
                    await asyncio.sleep(0.002)
                await asyncio.sleep(0.001)

        consumer_task = asyncio.create_task(delayed_consumer())

        try:
            metrics = await simulator.run()
        finally:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass

        # All 12 orders should eventually succeed thanks to retries and draining
        assert metrics.orders_generated == 12
        assert metrics.orders_submitted == 12
        assert metrics.orders_failed == 0
        assert metrics.buffer_full_events > 0, "Buffer full should have occurred"
        assert metrics.retry_count > 0, "Retries should have been performed"

    @pytest.mark.asyncio
    async def test_retry_limit_exhausted_continue_policy(self) -> None:
        """Test reaching max retries when no consumer drains buffer (continue policy)."""
        # Capacity 4 holds 3 items
        rb = RingBuffer(capacity=4)
        config = SimulatorConfig(
            total_orders=8,
            max_retries=3,
            retry_delay=0.001,
            on_retry_exhausted="continue",
        )

        simulator = MarketSimulator(ring_buffer=rb, config=config)
        metrics = await simulator.run()

        # 3 orders fit into buffer, remaining 5 fail after 3 retries each
        assert metrics.orders_generated == 8
        assert metrics.orders_submitted == 3
        assert metrics.orders_failed == 5
        assert metrics.retries_exhausted_events == 5
        assert metrics.retry_count == 5 * 3  # 15 retries
        assert len(metrics.failure_details) == 5

    @pytest.mark.asyncio
    async def test_retry_limit_exhausted_stop_policy(self) -> None:
        """Test reaching max retries halts simulator immediately when configured."""
        # Capacity 4 holds 3 items
        rb = RingBuffer(capacity=4)
        config = SimulatorConfig(
            total_orders=20,
            max_retries=2,
            retry_delay=0.001,
            on_retry_exhausted="stop",
        )

        simulator = MarketSimulator(ring_buffer=rb, config=config)
        metrics = await simulator.run()

        # 3 submit successfully, 4th order retries 2 times and fails, stopping simulation
        assert metrics.orders_generated == 4
        assert metrics.orders_submitted == 3
        assert metrics.orders_failed == 1
        assert metrics.retries_exhausted_events == 1
        assert metrics.retry_count == 2

    @pytest.mark.asyncio
    async def test_unexpected_submission_exception(self) -> None:
        """Test that non-BufferError exceptions in rb.put are logged and counted as failed."""
        mock_rb = MagicMock(spec=RingBuffer)
        # First call succeeds, second raises hardware/IO error, third succeeds
        mock_rb.put.side_effect = [None, OSError("Shared memory bus error"), None]

        config = SimulatorConfig(total_orders=3, random_seed=42)
        simulator = MarketSimulator(ring_buffer=mock_rb, config=config)
        metrics = await simulator.run()

        assert metrics.orders_generated == 3
        assert metrics.orders_submitted == 2
        assert metrics.orders_failed == 1
        assert "OSError" in metrics.failure_details[0][1]

    @pytest.mark.asyncio
    async def test_configurable_generation_rate(self) -> None:
        """Test that generation rate pacing works as expected."""
        rb = RingBuffer(capacity=20)
        # Target 20 orders/sec -> ~0.05s per order
        orders = 6
        target_rate = 20.0
        config = SimulatorConfig(
            total_orders=orders,
            orders_per_second=target_rate,
            random_seed=1,
        )

        simulator = MarketSimulator(ring_buffer=rb, config=config)
        metrics = await simulator.run()

        assert metrics.orders_submitted == orders
        # 6 orders paced at 0.05s intervals take at least ~0.25s
        assert metrics.total_execution_time_seconds >= 0.20

    @pytest.mark.asyncio
    async def test_modular_order_encoder(self) -> None:
        """Test passing an order_encoder to verify compatibility with future binary schemas."""
        rb = RingBuffer(capacity=10)

        # Mock binary encoder (e.g. simulating struct.pack)
        def mock_binary_encoder(order: Dict[str, Any]) -> bytes:
            side_byte = b"B" if order["side"] == "BUY" else b"S"
            return f"{order['order_id']}:{side_byte.decode()}:{order['price']}:{order['quantity']}".encode()

        config = SimulatorConfig(total_orders=3, random_seed=42)
        simulator = MarketSimulator(
            ring_buffer=rb,
            config=config,
            order_encoder=mock_binary_encoder,
        )
        metrics = await simulator.run()

        assert metrics.orders_submitted == 3
        # Check what was placed into the ring buffer
        raw_item = rb.get()
        assert isinstance(raw_item, bytes)
        assert b"1001" in raw_item

    @pytest.mark.asyncio
    async def test_cancellation(self) -> None:
        """Test cancelling the simulation prematurely."""
        rb = RingBuffer(capacity=100)
        config = SimulatorConfig(total_orders=1000, orders_per_second=10.0)
        simulator = MarketSimulator(ring_buffer=rb, config=config)

        # Cancel after 50ms
        async def cancel_later() -> None:
            await asyncio.sleep(0.05)
            simulator.cancel()

        cancel_task = asyncio.create_task(cancel_later())
        metrics = await simulator.run()
        await cancel_task

        assert metrics.orders_submitted < 1000

    def test_report_generation(self) -> None:
        """Test that generate_report outputs clear formatted statistics."""
        metrics = SimulationMetrics(
            orders_generated=100,
            orders_submitted=95,
            orders_failed=5,
            buffer_full_events=8,
            retry_count=6,
            retries_exhausted_events=5,
            total_execution_time_seconds=0.5,
            total_submission_latency_ns=950000,
            submission_latencies_ns=[10000] * 95,
        )
        report = metrics.generate_report()
        assert "CHRONOSMATCH MARKET SIMULATOR" in report
        assert "95" in report
        assert "ops/s" in report
        assert "Average submission latency" in report
