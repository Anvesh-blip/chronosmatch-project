"""ChronosMatch Multi-Process Simulation Runner.

Orchestrates cross-process producer-consumer trading execution on Windows using
the 'spawn' multiprocessing start method, file-backed mmap, and a shared multiprocessing.Lock.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from src.ipc.binary_ring_buffer import BinaryRingBuffer, BinaryRingBufferContext
logger = logging.getLogger("multiprocess_runner")


def producer_process_worker(
    file_path: str,
    lock: Any,
    config_dict: Dict[str, Any],
    result_queue: Any,
    ready_event: Any,
) -> None:
    """Target worker for the spawned producer process.

    Opens the shared mmap file, attaches with the shared lock, and executes
    the asynchronous MarketSimulator until all orders are submitted.
    """
    from src.simulator.market_simulator import (
        MarketSimulator,
        SimulatorConfig,
    )

    rb: Optional[BinaryRingBuffer] = None
    try:
        rb = BinaryRingBuffer.open(file_path=file_path, lock=lock)
        config = SimulatorConfig(**config_dict)
        simulator = MarketSimulator(ring_buffer=rb, config=config)

        if ready_event is not None:
            ready_event.set()

        metrics = asyncio.run(simulator.run())

        result_payload = {
            "orders_generated": metrics.orders_generated,
            "orders_submitted": metrics.orders_submitted,
            "orders_failed": metrics.orders_failed,
            "buffer_full_events": metrics.buffer_full_events,
            "retry_count": metrics.retry_count,
            "retries_exhausted_events": metrics.retries_exhausted_events,
            "total_execution_time_seconds": metrics.total_execution_time_seconds,
            "average_submission_latency_ns": metrics.average_submission_latency_ns,
            "average_submission_latency_us": metrics.average_submission_latency_us,
            "submitted_orders_per_second": metrics.submitted_orders_per_second,
            "status": "success",
        }
        result_queue.put(result_payload)
    except Exception as exc:
        logger.exception("Producer process crashed: %s", exc)
        result_queue.put({"status": "error", "error": f"{type(exc).__name__}: {exc}"})
    finally:
        if rb is not None:
            rb.close()


def consumer_process_worker(
    file_path: str,
    lock: Any,
    expected_orders: Optional[int],
    stop_event: Any,
    result_queue: Any,
    ready_event: Any,
    startup_delay: float = 0.0,
) -> None:
    """Target worker for the spawned consumer process.

    Simulates the downstream matching engine reading binary orders from the mmap ring buffer.
    """
    rb: Optional[BinaryRingBuffer] = None
    try:
        rb = BinaryRingBuffer.open(file_path=file_path, lock=lock)

        if ready_event is not None:
            ready_event.set()

        if startup_delay > 0:
            time.sleep(startup_delay)

        orders_drained = 0
        first_order_id: Optional[int] = None
        last_order_id: Optional[int] = None
        prev_order_id: Optional[int] = None
        fifo_violations = 0
        consecutive_empty = 0

        t_drain_start = time.perf_counter()

        while not stop_event.is_set():
            order = rb.get()
            if order is not None:
                orders_drained += 1
                consecutive_empty = 0
                oid = order["order_id"]
                if first_order_id is None:
                    first_order_id = oid
                if prev_order_id is not None and oid != prev_order_id + 1:
                    fifo_violations += 1
                prev_order_id = oid
                last_order_id = oid

                # If we were given an explicit target and reached it, we can stop
                if expected_orders is not None and orders_drained >= expected_orders:
                    break
            else:
                consecutive_empty += 1
                # Non-busy wait to prevent pegging CPU cores
                if consecutive_empty < 50:
                    time.sleep(0.0001)
                else:
                    time.sleep(0.001)

        # Final drain pass after stop signal
        while True:
            order = rb.get()
            if order is None:
                break
            orders_drained += 1
            oid = order["order_id"]
            if first_order_id is None:
                first_order_id = oid
            if prev_order_id is not None and oid != prev_order_id + 1:
                fifo_violations += 1
            prev_order_id = oid
            last_order_id = oid

        t_drain_end = time.perf_counter()
        drain_duration = t_drain_end - t_drain_start

        result_queue.put({
            "status": "success",
            "orders_drained": orders_drained,
            "first_order_id": first_order_id,
            "last_order_id": last_order_id,
            "fifo_violations": fifo_violations,
            "drain_time_seconds": drain_duration,
            "drain_rate_ops_per_second": (orders_drained / drain_duration) if drain_duration > 0 else 0.0,
        })
    except Exception as exc:
        logger.exception("Consumer process crashed: %s", exc)
        result_queue.put({"status": "error", "error": f"{type(exc).__name__}: {exc}"})
    finally:
        if rb is not None:
            rb.close()


def run_multiprocess_simulation(
    total_orders: int = 1000,
    orders_per_second: Optional[float] = None,
    random_seed: Optional[int] = 42,
    max_retries: int = 5,
    retry_delay: float = 0.001,
    on_retry_exhausted: str = "continue",
    consumer_startup_delay: float = 0.0,
    file_path: Optional[str] = None,
    timeout: float = 30.0,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Run an end-to-end multi-process simulation using the spawn method.

    Returns:
        Tuple of (producer_results_dict, consumer_results_dict)
    """
    ctx = multiprocessing.get_context("spawn")
    shared_lock = ctx.Lock()
    ready_producer = ctx.Event()
    ready_consumer = ctx.Event()
    stop_consumer = ctx.Event()

    producer_q = ctx.Queue()
    consumer_q = ctx.Queue()

    config_dict = {
        "total_orders": total_orders,
        "orders_per_second": orders_per_second,
        "random_seed": random_seed,
        "max_retries": max_retries,
        "retry_delay": retry_delay,
        "on_retry_exhausted": on_retry_exhausted,
    }

    with BinaryRingBufferContext(file_path=file_path, lock=shared_lock) as parent_rb:
        str_path = str(parent_rb.file_path)

        consumer_p = ctx.Process(
            target=consumer_process_worker,
            args=(
                str_path,
                shared_lock,
                total_orders,
                stop_consumer,
                consumer_q,
                ready_consumer,
                consumer_startup_delay,
            ),
            name="ChronosConsumerProcess",
        )
        producer_p = ctx.Process(
            target=producer_process_worker,
            args=(str_path, shared_lock, config_dict, producer_q, ready_producer),
            name="ChronosProducerProcess",
        )

        consumer_p.start()
        producer_p.start()

        # Wait for both processes to signal readiness
        ready_consumer.wait(timeout=5.0)
        ready_producer.wait(timeout=5.0)

        t_e2e_start = time.perf_counter()

        # Wait for producer to finish
        producer_p.join(timeout=timeout)
        if producer_p.is_alive():
            producer_p.terminate()
            raise TimeoutError("Producer process timed out during simulation")

        # Allow consumer time to drain, then signal stop
        stop_consumer.set()
        consumer_p.join(timeout=5.0)
        if consumer_p.is_alive():
            consumer_p.terminate()

        t_e2e_end = time.perf_counter()
        e2e_duration = t_e2e_end - t_e2e_start

        # Retrieve payloads from queues
        producer_res: Dict[str, Any] = (
            producer_q.get(timeout=2.0) if not producer_q.empty() else {"status": "no_output"}
        )
        consumer_res: Dict[str, Any] = (
            consumer_q.get(timeout=2.0) if not consumer_q.empty() else {"status": "no_output"}
        )

        # Attach end-to-end pipeline metrics
        consumer_res["end_to_end_duration_seconds"] = e2e_duration
        drained = consumer_res.get("orders_drained", 0)
        consumer_res["end_to_end_throughput_ops_per_second"] = (
            (drained / e2e_duration) if e2e_duration > 0 else 0.0
        )

    return producer_res, consumer_res
