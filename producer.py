from src.ipc.shared_ring_buffer import (
    open_shared_memory,
    put_order_locked,
)


def producer(lock):
    file_handle, memory = open_shared_memory("shared_orders.dat")

    try:
        put_order_locked(lock, memory, 2002, 15025, 10, 0)
        memory.flush()

        print("Order written successfully!")
        print("Order: ID=2002, Price=15025, Quantity=10, Side=BUY")
    finally:
        memory.close()
        file_handle.close()