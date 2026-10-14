
import mmap
import multiprocessing
import time

from src.ipc.shared_ring_buffer import (
    BUFFER_SIZE,
    initialize_header,
    open_shared_memory,
    put_order_locked,
    get_order_locked,
)


def producer(lock, count):
    file_handle, memory = open_shared_memory()
    try:
        for i in range(count):
            while True:
                try:
                    put_order_locked(lock, memory, i, 15025, 1, 0)
                    break
                except BufferError:
                    time.sleep(0.001)
    finally:
        memory.close()
        file_handle.close()


def consumer(lock, count, results):
    file_handle, memory = open_shared_memory()
    try:
        while len(results) < count:
            order = get_order_locked(lock, memory)
            if order is not None:
                results.append(order)
            else:
                time.sleep(0.001)
    finally:
        memory.close()
        file_handle.close()

if __name__ == "__main__":
    multiprocessing.freeze_support()

    file_handle, memory = open_shared_memory()
    initialize_header(memory)
    memory.flush()
    memory.close()
    file_handle.close()

    lock = multiprocessing.Lock()
    manager = multiprocessing.Manager()
    results = manager.list()
    count = 100

    p = multiprocessing.Process(
        target=producer, args=(lock, count)
    )
    c = multiprocessing.Process(
        target=consumer, args=(lock, count, results)
    )

    c.start()
    p.start()

    p.join(timeout=10)
    c.join(timeout=10)

    if p.is_alive():
        p.terminate()
        p.join()
        raise RuntimeError("Producer process timed out")

    if c.is_alive():
        c.terminate()
        c.join()
        raise RuntimeError("Consumer process timed out")

    if p.exitcode != 0:
        raise RuntimeError(f"Producer failed: exit code {p.exitcode}")

    if c.exitcode != 0:
        raise RuntimeError(f"Consumer failed: exit code {c.exitcode}")

    print("Orders received:", len(results))
    print("First order:", results[0] if results else None)
    print("Last order:", results[-1] if results else None)
    print(
        "All order IDs correct:",
        [o[0] for o in results] == list(range(count)),
    )

    manager.shutdown()
