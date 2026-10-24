import multiprocessing

from src.ipc.shared_ring_buffer import create_shared_memory
from producer import producer
from consumer import consumer


def main():
    # Initialize the shared-memory buffer only once.
    file_handle, memory = create_shared_memory("shared_orders.dat")
    memory.close()
    file_handle.close()

    # Create one lock shared by both processes.
    context = multiprocessing.get_context("spawn")
    lock = context.Lock()

    # Start producer and consumer as separate processes.
    producer_process = context.Process(target=producer, args=(lock,))
    consumer_process = context.Process(target=consumer, args=(lock,))

    producer_process.start()
    producer_process.join()

    consumer_process.start()
    consumer_process.join()

    # Check that both processes completed successfully.
    if producer_process.exitcode != 0:
        raise RuntimeError(
        f"Producer process failed: {producer_process.exitcode}"
    )

    if consumer_process.exitcode != 0:
        raise RuntimeError(
        f"Consumer process failed: {consumer_process.exitcode}"
    )

if __name__ == "__main__":
    main()