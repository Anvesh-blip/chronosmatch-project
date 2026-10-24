import time

from src.ipc.shared_ring_buffer import (
    open_shared_memory,
    get_order_locked,
)


STOP_ORDER_ID = 0


def consumer(lock):
    file_handle, memory = open_shared_memory("shared_orders.dat")

    try:
        print("Consumer started. Waiting for orders...")

        while True:
            order = get_order_locked(lock, memory)

            if order is None:
                time.sleep(0.001)
                continue

            order_id, price_ticks, quantity, side = order

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
