from src.ipc.shared_ring_buffer import (
    open_shared_memory,
    get_order_locked,
)


def consumer(lock):
    file_handle, memory = open_shared_memory("shared_orders.dat")

    try:
        order = get_order_locked(lock, memory)

        if order is None:
            print("No orders available.")
        else:
            order_id, price_ticks, quantity, side = order

            print("Order ID:", order_id)
            print("Price ticks:", price_ticks)
            print("Quantity:", quantity)
            print("Side:", "BUY" if side == 0 else "SELL")
    finally:
        memory.close()
        file_handle.close()