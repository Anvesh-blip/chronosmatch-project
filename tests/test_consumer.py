import multiprocessing
import time

from consumer import consumer
from src.ipc.shared_ring_buffer import create_shared_memory


def test_consumer_exits_after_stop_timeout(tmp_path, monkeypatch):
    filename = str(tmp_path / "shared_orders.dat")

    file_handle, memory = create_shared_memory(filename)
    memory.close()
    file_handle.close()

    # Make the consumer open our temporary test file.
    import consumer as consumer_module

    original_open = consumer_module.open_shared_memory

    monkeypatch.setattr(
        consumer_module,
        "open_shared_memory",
        lambda _: original_open(filename),
    )

    context = multiprocessing.get_context("spawn")
    lock = context.Lock()

    # The consumer should exit when no stop signal arrives.
    start = time.monotonic()
    consumer(lock, stop_timeout=0.1)
    elapsed = time.monotonic() - start

    assert elapsed < 2