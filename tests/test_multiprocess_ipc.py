import multiprocessing
import time

from src.ipc.shared_ring_buffer import (
    create_shared_memory,
    open_shared_memory,
    put_order_locked,
    get_order_locked,
)


def producer_process(filename, lock, ready):
    file_handle, memory = open_shared_memory(filename)
    try:
        put_order_locked(lock, memory, 2002, 15025, 10, 0)
        memory.flush()
        ready.set()
    finally:
        memory.close()
        file_handle.close()


def consumer_process(filename, lock, ready, result):
    if not ready.wait(timeout=5):
        result.put("TIMEOUT")
        return

    file_handle, memory = open_shared_memory(filename)
    try:
        order = get_order_locked(lock, memory)
        result.put(order)
    finally:
        memory.close()
        file_handle.close()


def test_two_processes_share_order(tmp_path):
    filename = str(tmp_path / "multiprocess_orders.dat")

    file_handle, memory = create_shared_memory(filename)
    memory.close()
    file_handle.close()

    context = multiprocessing.get_context("spawn")
    lock = context.Lock()
    ready = context.Event()
    result = context.Queue()

    producer = context.Process(
        target=producer_process,
        args=(filename, lock, ready),
    )
    consumer = context.Process(
        target=consumer_process,
        args=(filename, lock, ready, result),
    )

    try:
        producer.start()
        consumer.start()

        producer.join(timeout=10)
        consumer.join(timeout=10)

        assert not producer.is_alive(), "Producer process hung"
        assert not consumer.is_alive(), "Consumer process hung"
        assert producer.exitcode == 0
        assert consumer.exitcode == 0
        assert result.get(timeout=2) == (2002, 15025, 10, 0)
    finally:
        for process in (producer, consumer):
            if process.is_alive():
                process.terminate()
            process.join()

        result.close()