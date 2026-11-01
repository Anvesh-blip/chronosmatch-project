import time

from src.ipc.shared_ring_buffer import (
    open_shared_memory,
    put_order_locked,
)

MAX_RETRIES = 1000
RETRY_DELAY = 0.001


def submit_order(lock, memory, order):
    for attempt in range(MAX_RETRIES):
        try:
            put_order_locked(lock, memory, *order)
            return
        except BufferError:
            time.sleep(RETRY_DELAY)

    raise BufferError(f"Could not submit order: {order}")


def producer(lock):
    file_handle, memory = open_shared_memory("shared_orders.dat")

    try:
        orders = [
            (2002, 15025, 10, 0),
            (2003, 15100, 5, 1),
            (2004, 15250, 8, 0),
        ]

        for order in orders:
            submit_order(lock, memory, order)
            memory.flush()
            print(f"Submitted order: {order}")

        stop_order = (0, 1, 1, 0)
        submit_order(lock, memory, stop_order)
        memory.flush()
        print("Stop signal submitted.")

    finally:
        memory.close()
        file_handle.close()