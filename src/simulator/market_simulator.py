
"""CHRONOSMATCH Market Simulator.

Role: Arjit — Async Market Simulator Engineer.
Generates simulated BUY/SELL orders asynchronously and submits them
through the shared RingBuffer IPC interface.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.ipc.ring_buffer import RingBuffer

logger = logging.getLogger("market_simulator")


@dataclass
class SimulatorConfig:
    """Configuration options for the Market Simulator."""

    total_orders: int = 1000
    orders_per_second: Optional[float] = None
    random_seed: Optional[int] = None
    max_retries: int = 5
    retry_delay: float = 0.001
    on_retry_exhausted: str = "continue"
    start_order_id: int = 1001
    min_price: float = 10.0
    max_price: float = 500.0
    price_decimals: int = 2
    min_quantity: int = 1
    max_quantity: int = 100

    def __post_init__(self) -> None:
        if self.total_orders < 0:
            raise ValueError(f"total_orders must be non-negative, got {self.total_orders}")
        if self.orders_per_second is not None and self.orders_per_second <= 0:
            raise ValueError(f"orders_per_second must be positive, got {self.orders_per_second}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be non-negative, got {self.max_retries}")
        if self.retry_delay < 0:
            raise ValueError(f"retry_delay must be non-negative, got {self.retry_delay}")
        if self.on_retry_exhausted not in ("continue", "stop"):
            raise ValueError(
                f"on_retry_exhausted must be 'continue' or 'stop', got {self.on_retry_exhausted}"
            )
        if self.min_price <= 0:
            raise ValueError(f"min_price must be positive, got {self.min_price}")
        if self.max_price < self.min_price:
            raise ValueError(
                f"max_price ({self.max_price}) cannot be less than min_price ({self.min_price})"
            )
        if self.min_quantity <= 0:
            raise ValueError(f"min_quantity must be positive, got {self.min_quantity}")
        if self.max_quantity < self.min_quantity:
            raise ValueError(
                f"max_quantity ({self.max_quantity}) cannot be less than min_quantity ({self.min_quantity})"
            )


@dataclass
class SimulationMetrics:
    """Tracks order-generation and submission statistics."""

    orders_generated: int = 0
    orders_submitted: int = 0
    orders_failed: int = 0
    buffer_full_events: int = 0
    retry_count: int = 0
    retries_exhausted_events: int = 0
    total_execution_time_seconds: float = 0.0
    total_submission_latency_ns: int = 0
    submission_latencies_ns: List[int] = field(default_factory=list)
    failure_details: List[Tuple[Dict[str, Any], str]] = field(default_factory=list)

    @property
    def submitted_orders_per_second(self) -> float:
        if self.total_execution_time_seconds > 0:
            return self.orders_submitted / self.total_execution_time_seconds
        return 0.0

    @property
    def average_submission_latency_ns(self) -> float:
        if self.orders_submitted > 0:
            return self.total_submission_latency_ns / self.orders_submitted
        return 0.0

    @property
    def average_submission_latency_us(self) -> float:
        return self.average_submission_latency_ns / 1000.0

    def record_generated(self) -> None:
        self.orders_generated += 1

    def record_success(self, latency_ns: int) -> None:
        self.orders_submitted += 1
        self.total_submission_latency_ns += latency_ns
        if len(self.submission_latencies_ns) < 100000:
            self.submission_latencies_ns.append(latency_ns)

    def record_buffer_full(self) -> None:
        self.buffer_full_events += 1

    def record_retry(self) -> None:
        self.retry_count += 1

    def record_exhausted(self) -> None:
        self.retries_exhausted_events += 1

    def record_failure(self, order: Dict[str, Any], reason: str) -> None:
        self.orders_failed += 1
        self.failure_details.append((order, reason))

    def generate_report(self) -> str:
        avg_ns = self.average_submission_latency_ns
        avg_us = self.average_submission_latency_us
        t_sec = self.total_execution_time_seconds
        rate = self.submitted_orders_per_second

        p50_us = 0.0
        p95_us = 0.0
        p99_us = 0.0

        if self.submission_latencies_ns:
            sorted_lat = sorted(self.submission_latencies_ns)
            n = len(sorted_lat)
            p50_us = sorted_lat[int(0.50 * n)] / 1000.0
            p95_us = sorted_lat[min(int(0.95 * n), n - 1)] / 1000.0
            p99_us = sorted_lat[min(int(0.99 * n), n - 1)] / 1000.0

        border = "=" * 65
        subborder = "-" * 65

        lines = [
            border,
            " CHRONOSMATCH MARKET SIMULATOR - PERFORMANCE REPORT",
            " Engineer: Arjit | Module: Async Market Simulator",
            border,
            f" Total orders generated:           {self.orders_generated:>12,d}",
            f" Total orders submitted (success): {self.orders_submitted:>12,d}",
            f" Total orders failed:              {self.orders_failed:>12,d}",
            subborder,
            f" Buffer-full events (BufferError): {self.buffer_full_events:>12,d}",
            f" Retry attempts made:              {self.retry_count:>12,d}",
            f" Retries exhausted events:         {self.retries_exhausted_events:>12,d}",
            subborder,
            f" Total execution time:             {t_sec:>12.4f} s",
            f" Actual submitted orders / sec:    {rate:>12.2f} ops/s",
            subborder,
            f" Average submission latency:       {avg_us:>12.3f} us ({avg_ns:,.1f} ns)",
        ]

        if self.submission_latencies_ns:
            lines.extend([
                f" Median latency (p50):             {p50_us:>12.3f} us",
                f" 95th percentile latency (p95):    {p95_us:>12.3f} us",
                f" 99th percentile latency (p99):    {p99_us:>12.3f} us",
            ])

        lines.append(border)
        return "\n".join(lines)


class OrderGenerator:
    """Generates synthetic BUY and SELL market orders."""

    VALID_SIDES = ("BUY", "SELL")

    def __init__(
        self,
        start_order_id: int = 1001,
        min_price: float = 10.0,
        max_price: float = 500.0,
        price_decimals: int = 2,
        min_quantity: int = 1,
        max_quantity: int = 100,
        seed: Optional[int] = None,
    ) -> None:
        self._current_order_id = start_order_id
        self.min_price = min_price
        self.max_price = max_price
        self.price_decimals = price_decimals
        self.min_quantity = min_quantity
        self.max_quantity = max_quantity
        self.seed = seed
        self._rng = random.Random(seed)

    def reset(self, start_order_id: Optional[int] = None) -> None:
        if start_order_id is not None:
            self._current_order_id = start_order_id
        self._rng = random.Random(self.seed)

    def generate_order(self) -> Dict[str, Any]:
        order_id = self._current_order_id
        self._current_order_id += 1

        side = self._rng.choice(self.VALID_SIDES)
        raw_price = self._rng.uniform(self.min_price, self.max_price)
        price = round(raw_price, self.price_decimals)

        if price <= 0:
            price = 0.01

        quantity = self._rng.randint(self.min_quantity, self.max_quantity)

        return {
            "order_id": order_id,
            "side": side,
            "price": price,
            "quantity": quantity,
        }


class MarketSimulator:
    """Asynchronous Market Simulator for ChronosMatch."""

    def __init__(
        self,
        ring_buffer: RingBuffer,
        config: Optional[SimulatorConfig] = None,
        order_encoder: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> None:
        self.ring_buffer = ring_buffer
        self.config = config or SimulatorConfig()
        self.order_encoder = order_encoder

        self.generator = OrderGenerator(
            start_order_id=self.config.start_order_id,
            min_price=self.config.min_price,
            max_price=self.config.max_price,
            price_decimals=self.config.price_decimals,
            min_quantity=self.config.min_quantity,
            max_quantity=self.config.max_quantity,
            seed=self.config.random_seed,
        )

        self.metrics = SimulationMetrics()
        self._running = False
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    async def submit_order(self, order: Dict[str, Any]) -> bool:
        payload = self.order_encoder(order) if self.order_encoder else order
        retries = 0

        while not self._cancelled:
            try:
                t_before = time.perf_counter_ns()
                self.ring_buffer.put(payload)
                t_after = time.perf_counter_ns()

                latency_ns = t_after - t_before
                self.metrics.record_success(latency_ns)
                return True

            except BufferError:
                self.metrics.record_buffer_full()

                if retries < self.config.max_retries:
                    retries += 1
                    self.metrics.record_retry()
                    logger.debug(
                        "Buffer full on order %s, retry %d/%d after %.4fs",
                        order["order_id"],
                        retries,
                        self.config.max_retries,
                        self.config.retry_delay,
                    )
                    await asyncio.sleep(self.config.retry_delay)
                else:
                    self.metrics.record_exhausted()
                    reason = (
                        f"Buffer full: retry limit of {self.config.max_retries} reached"
                    )
                    logger.warning(
                        "Order %s failed submission: %s", order["order_id"], reason
                    )
                    self.metrics.record_failure(order, reason)
                    return False

            except Exception as exc:
                logger.error(
                    "Unexpected error submitting order %s: %s",
                    order["order_id"],
                    exc,
                    exc_info=True,
                )
                self.metrics.record_failure(
                    order, f"Exception: {type(exc).__name__}: {exc}"
                )
                return False

        self.metrics.record_failure(order, "Simulation cancelled")
        return False

    async def run(self) -> SimulationMetrics:
        self.metrics = SimulationMetrics()
        self._running = True
        self._cancelled = False

        self.generator.reset(start_order_id=self.config.start_order_id)

        nominal_interval = 0.0
        if self.config.orders_per_second and self.config.orders_per_second > 0:
            nominal_interval = 1.0 / self.config.orders_per_second

        logger.info(
            "Starting Market Simulator: total_orders=%d, rate=%s ops/s, seed=%s",
            self.config.total_orders,
            self.config.orders_per_second if self.config.orders_per_second else "unlimited",
            self.config.random_seed,
        )

        sim_start_time = time.perf_counter()

        try:
            for idx in range(self.config.total_orders):
                if self._cancelled:
                    logger.info("Simulation run was cancelled by caller.")
                    break

                order = self.generator.generate_order()
                self.metrics.record_generated()

                success = await self.submit_order(order)

                if not success and self.config.on_retry_exhausted == "stop":
                    logger.warning(
                        "Stopping simulation early due to exhausted retries on order %s",
                        order["order_id"],
                    )
                    break

                if nominal_interval > 0:
                    await asyncio.sleep(nominal_interval)
                elif (idx + 1) % 128 == 0:
                    await asyncio.sleep(0)

        finally:
            sim_end_time = time.perf_counter()
            self.metrics.total_execution_time_seconds = sim_end_time - sim_start_time
            self._running = False

        logger.info(
            "Market Simulator completed: %d generated, %d submitted, %d failed in %.4f s",
            self.metrics.orders_generated,
            self.metrics.orders_submitted,
            self.metrics.orders_failed,
            self.metrics.total_execution_time_seconds,
        )

        return self.metrics


async def _drain_buffer_consumer(
    ring_buffer: RingBuffer,
    stop_event: asyncio.Event,
    consumed_counter: List[int],
    drain_rate: Optional[float] = None,
) -> None:
    """Background consumer that drains orders from the RingBuffer."""

    interval = 1.0 / drain_rate if drain_rate and drain_rate > 0 else 0.0

    while not stop_event.is_set():
        item = ring_buffer.get()

        if item is not None:
            consumed_counter[0] += 1
            if interval > 0:
                await asyncio.sleep(interval)
        else:
            await asyncio.sleep(0.0001)


def parse_arguments(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ChronosMatch Market Simulator (Arjit's Module)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--orders", type=int, default=1000,
                        help="Total number of orders to generate")
    parser.add_argument("--rate", type=float, default=None,
                        help="Target submission rate in orders per second (None = unthrottled)")
    parser.add_argument("--buffer-capacity", type=int, default=2048,
                        help="Capacity for the RingBuffer (must be >= 2)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducible order sequences")
    parser.add_argument("--max-retries", type=int, default=5,
                        help="Maximum buffer-full retry attempts before marking order failed")
    parser.add_argument("--retry-delay", type=float, default=0.001,
                        help="Delay in seconds before retrying a full buffer")
    parser.add_argument("--on-exhausted", choices=["continue", "stop"],
                        default="continue",
                        help="Action when retries are exhausted for an order")
    parser.add_argument("--with-consumer", action="store_true",
                        help="Run a concurrent background consumer task to drain orders")
    parser.add_argument("--ipc", choices=["list", "binary"], default="list",
                        help="IPC buffer implementation: 'list' or 'binary'")
    parser.add_argument("--multiprocess", action="store_true",
                        help="Run real cross-process simulation using Windows spawn")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose debug logging")

    return parser.parse_args(argv)


async def _run_async_simulation(args: argparse.Namespace, ring_buffer: Any) -> int:
    config = SimulatorConfig(
        total_orders=args.orders,
        orders_per_second=args.rate,
        random_seed=args.seed,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
        on_retry_exhausted=args.on_exhausted,
    )

    simulator = MarketSimulator(ring_buffer=ring_buffer, config=config)

    consumer_task = None
    stop_consumer = asyncio.Event()
    consumed = [0]

    if args.with_consumer:
        consumer_task = asyncio.create_task(
            _drain_buffer_consumer(ring_buffer, stop_consumer, consumed)
        )

    try:
        metrics = await simulator.run()
    finally:
        if consumer_task is not None:
            await asyncio.sleep(0.01)
            stop_consumer.set()
            await consumer_task

    print()
    print(metrics.generate_report())

    if args.with_consumer:
        print(f" Consumer drained orders:          {consumed[0]:>12,d}")
        print("=" * 65)

    return 0


def _run_multiprocess_cli(args: argparse.Namespace) -> int:
    """Run cross-process simulation using Windows spawn."""

    from src.ipc.multiprocess_runner import run_multiprocess_simulation

    print(" Launching cross-process simulation (Windows spawn method)...")
    print(" Producer: MarketSimulator process | Consumer: Matching engine reader process")
    print("=" * 65)

    prod_res, cons_res = run_multiprocess_simulation(
        total_orders=args.orders,
        orders_per_second=args.rate,
        random_seed=args.seed,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
        on_retry_exhausted=args.on_exhausted,
    )

    border = "=" * 65
    subborder = "-" * 65

    print()
    print(border)
    print(" CHRONOSMATCH MULTI-PROCESS SIMULATOR - PERFORMANCE REPORT")
    print(" Mode: Cross-Process Binary IPC (Windows spawn + 16-byte struct)")
    print(border)

    if prod_res.get("status") == "success":
        print(f" Total orders generated:           {prod_res['orders_generated']:>12,d}")
        print(f" Total orders submitted (success): {prod_res['orders_submitted']:>12,d}")
        print(f" Total orders failed:              {prod_res['orders_failed']:>12,d}")
        print(subborder)
        print(f" Buffer-full events (BufferError): {prod_res['buffer_full_events']:>12,d}")
        print(f" Retry attempts made:              {prod_res['retry_count']:>12,d}")
        print(f" Retries exhausted events:         {prod_res['retries_exhausted_events']:>12,d}")
        print(subborder)
        print(
            f" Total execution time:             "
            f"{prod_res['total_execution_time_seconds']:>12.4f} s"
        )
        print(
            f" Actual submitted orders / sec:    "
            f"{prod_res['submitted_orders_per_second']:>12.2f} ops/s"
        )
        print(subborder)
        print(
            f" Average submission latency:       "
            f"{prod_res['average_submission_latency_us']:>12.3f} us "
            f"({prod_res['average_submission_latency_ns']:,.1f} ns)"
        )
    else:
        print(f" Producer error: {prod_res.get('error')}")

    print(subborder)

    if cons_res.get("status") == "success":
        fifo_v = cons_res.get("fifo_violations", 0)
        e2e_time = cons_res.get("end_to_end_duration_seconds", 0.0)
        e2e_tput = cons_res.get("end_to_end_throughput_ops_per_second", 0.0)
        drain_rate = cons_res.get("drain_rate_ops_per_second", 0.0)

        # Matching-engine statistics from the consumer process
        trades_executed = cons_res.get("trades_executed", 0)
        total_traded_quantity = cons_res.get("total_traded_quantity", 0)

        print(f" Consumer drained orders:          {cons_res['orders_drained']:>12,d}")
        print(f" First order ID received:          {cons_res.get('first_order_id')}")
        print(f" Last order ID received:           {cons_res.get('last_order_id')}")
        print(
            f" FIFO sequence integrity:          "
            f"{'0 violations (Strict FIFO preserved)' if fifo_v == 0 else f'{fifo_v} VIOLATIONS'}"
        )

        print(subborder)
        print(" MATCHING ENGINE RESULTS:")
        print(f" Executed trades:                  {trades_executed:>12,d}")
        print(f" Total traded quantity:            {total_traded_quantity:>12,d}")

        print(subborder)
        print(" THROUGHPUT BREAKDOWN:")
        print(
            f" - Producer Ingestion Throughput:  "
            f"{prod_res.get('submitted_orders_per_second', 0.0):>12.2f} ops/s"
        )
        print(f" - Consumer Drain Throughput:      {drain_rate:>12.2f} ops/s")
        print(
            f" - End-to-End Pipeline Throughput: "
            f"{e2e_tput:>12.2f} ops/s (wall: {e2e_time:.4f}s)"
        )
    else:
        print(f" Consumer error: {cons_res.get('error')}")

    print(border)

    return 0


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    ipc_mode_str = (
        "BINARY (Cross-Process Spawn)"
        if args.multiprocess
        else f"{args.ipc.upper()} (Single-Process Async)"
    )

    print("=" * 65)
    print(" CHRONOSMATCH - Zero-Copy High-Frequency Trading Engine")
    print(" Module: Async Market Simulator (Engineer: Arjit)")
    print(f" IPC Mode: {ipc_mode_str}")
    print("=" * 65)

    if args.multiprocess:
        return _run_multiprocess_cli(args)

    if args.ipc == "binary":
        from src.ipc.binary_ring_buffer import BinaryRingBufferContext

        with BinaryRingBufferContext() as binary_rb:
            return await _run_async_simulation(args, binary_rb)

    ring_buffer = RingBuffer(capacity=args.buffer_capacity)
    return await _run_async_simulation(args, ring_buffer)


def main() -> None:
    args = parse_arguments()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
