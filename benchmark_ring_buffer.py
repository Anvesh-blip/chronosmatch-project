
import time

from src.ipc.shared_ring_buffer import (
    BUFFER_SIZE,
    open_shared_memory,
    initialize_header,
    put_order,
    get_order,
)

filename = "shared_orders.dat"

# Create the file with the required size
with open(filename, "wb") as f:
    f.truncate(BUFFER_SIZE)

file_handle, memory = open_shared_memory(filename)

try:
    initialize_header(memory)

    total_orders = 100_000

    start = time.perf_counter()

    for order_id in range(total_orders):
        put_order(memory, order_id, 15025, 1, 0)

        order = get_order(memory)

        if order != (order_id, 15025, 1, 0):
            raise ValueError(f"Incorrect order: {order}")

    end = time.perf_counter()

    elapsed = end - start

    print("Total orders:", total_orders)
    print("Total time:", elapsed, "seconds")
    print("Orders per second:", total_orders / elapsed)

finally:
    memory.close()
    file_handle.close()
