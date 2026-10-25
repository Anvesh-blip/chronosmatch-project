import time

from src.ipc.shared_ring_buffer import (
    open_shared_memory,
    get_order_locked,
)


STOP_ORDER_ID = 0
STOP_TIMEOUT_SECONDS = 10


def consumer(lock,stop_timeout=STOP_TIMEOUT_SECONDS):
    file_handle, memory = open_shared_memory("shared_orders.dat")

    try:
        print("Consumer started. Waiting for orders...")
        stop_deadline = time.monotonic() + stop_timeout

        while True:
            order = get_order_locked(lock, memory)

            if order is None:
                if time.monotonic() >= stop_deadline:
                    print("Stop signal timeout. Consumer exiting.")
                    break

                time.sleep(0.001)
                continue

            order_id, price_ticks, quantity, side = order
            stop_deadline = time.monotonic() + stop_timeout

            # A special order ID tells the consumer to stop.
            if order_id == STOP_ORDER_ID:
                print("Stop signal received. Consumer exiting.")
                break

            print(
                f"Order ID: {order_id}, "
                f"Price ticks: {price_ticks}, "
                f"Quantity: {quantity}, "
                f"Side: {'BUY' if side == 0 else 'SELL'}"
            )

    finally:
        memory.close()
        file_handle.close()
