from src.ipc.shared_ring_buffer import (
    open_shared_memory,
    put_order_locked,
)


def producer(lock):
    file_handle, memory = open_shared_memory("shared_orders.dat")

    try:
        orders = [
            (2002, 15025, 10, 0),  # BUY
            (2003, 15100, 5, 1),   # SELL
            (2004, 15250, 8, 0),   # BUY
        ]

        for order in orders:
            put_order_locked(lock, memory, *order)
            memory.flush()
            print(f"Submitted order: {order}")

        # Send the stop signal after all normal orders.
        put_order_locked(lock, memory, 0, 1, 1, 0)
        memory.flush()
        print("Stop signal submitted.")

    finally:
        memory.close()
        file_handle.close()
