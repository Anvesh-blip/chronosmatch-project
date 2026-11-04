import multiprocessing
from src.ipc.shared_ring_buffer import (
    create_shared_memory,
    open_shared_memory,
    put_order_locked,
    get_order_locked,
)

TOTAL_ORDERS = 500


def producer_process(filename, lock, ready):
    file_handle, memory = open_shared_memory(filename)

    try:
        for order_id in range(1, TOTAL_ORDERS + 1):
            order = (order_id, 15025, 10, 0)
            put_order_locked(lock, memory, *order)

        memory.flush()
        ready.set()

    finally:
        memory.close()
        file_handle.close()


def consumer_process(filename, lock, ready, result):
    if not ready.wait(timeout=10):
        result.put("TIMEOUT")
        return

    file_handle, memory = open_shared_memory(filename)

    try:
        orders = []

        for _ in range(TOTAL_ORDERS):
            order = get_order_locked(lock, memory)
            orders.append(order)

        result.put(orders)

    finally:
        memory.close()
        file_handle.close()


def test_two_processes_share_500_orders(tmp_path):
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

        producer.join(timeout=15)
        consumer.join(timeout=15)

        assert not producer.is_alive(), "Producer process hung"
        assert not consumer.is_alive(), "Consumer process hung"
        assert producer.exitcode == 0
        assert consumer.exitcode == 0

        received_orders = result.get(timeout=3)

        expected_orders = [
            (order_id, 15025, 10, 0)
            for order_id in range(1, TOTAL_ORDERS + 1)
        ]

        assert received_orders == expected_orders
        assert len(received_orders) == TOTAL_ORDERS
        assert len(set(received_orders)) == TOTAL_ORDERS

    finally:
        for process in (producer, consumer):
            if process.is_alive():
                process.terminate()
            process.join()

        result.close()