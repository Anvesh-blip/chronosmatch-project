import time

from src.ipc.shared_ring_buffer import (
    open_shared_memory,
    get_order_locked,
)


def consumer(lock):
    file_handle, memory = open_shared_memory("shared_orders.dat")

    try:
        order = None

        # Wait up to 5 seconds for an order.
        deadline = time.monotonic() + 5

        while order is None and time.monotonic() < deadline:
            order = get_order_locked(lock, memory)

            if order is None:
                time.sleep(0.001)

        if order is None:
            print("No orders received within 5 seconds.")
            return

        order_id, price_ticks, quantity, side = order

        print("Order ID:", order_id)
        print("Price ticks:", price_ticks)
        print("Quantity:", quantity)
        print("Side:", "BUY" if side == 0 else "SELL")

    finally:
        memory.close()
        file_handle.close()