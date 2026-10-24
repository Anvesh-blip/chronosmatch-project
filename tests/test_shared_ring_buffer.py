import mmap
import pytest

from src.ipc.shared_ring_buffer import (
    BUFFER_SIZE,
    CAPACITY,
    initialize_header,
    put_order,
    get_order,
)
from multiprocessing import Lock
from src.ipc.shared_ring_buffer import put_order_locked, get_order_locked


def create_memory():
    memory = mmap.mmap(-1, BUFFER_SIZE)
    initialize_header(memory)
    return memory


def test_put_and_get_order():
    memory = create_memory()
    try:
        put_order(memory, 2002, 15025, 10, 0)
        assert get_order(memory) == (2002, 15025, 10, 0)
        assert get_order(memory) is None
    finally:
        memory.close()


def test_buffer_full():
    memory = create_memory()
    try:
        for i in range(CAPACITY - 1):
            put_order(memory, i, 15025, 1, 0)

        with pytest.raises(BufferError):
            put_order(memory, 9999, 15025, 1, 0)
    finally:
        memory.close()


def test_wrap_around():
    memory = create_memory()
    try:
        for i in range(3):
            put_order(memory, i, 15025, 1, 0)

        assert get_order(memory) == (0, 15025, 1, 0)
        assert get_order(memory) == (1, 15025, 1, 0)

        put_order(memory, 100, 16000, 2, 1)
        put_order(memory, 101, 17000, 3, 0)

        assert get_order(memory) == (2, 15025, 1, 0)
        assert get_order(memory) == (100, 16000, 2, 1)
        assert get_order(memory) == (101, 17000, 3, 0)
        assert get_order(memory) is None
    finally:
        memory.close()

@pytest.mark.parametrize(
    "order_id, price_ticks, quantity, side, error",
    [
        (-1, 15025, 1, 0, ValueError),
        (1, 0, 1, 0, ValueError),
        (1, 15025, 0, 0, ValueError),
        (1, 15025, 1, 2, ValueError),
        ("abc", 15025, 1, 0, TypeError),
        (1, 15025, -1, 0, ValueError),
        (1, 15025, 1, 0.5, TypeError),
        (1, 15025, 1, True, TypeError),
        (1, 15025, 1 << 32, 0, ValueError),
    ],
)
def test_invalid_orders(
    order_id, price_ticks, quantity, side, error
):
    memory = create_memory()
    try:
        with pytest.raises(error):
            put_order(memory, order_id, price_ticks, quantity, side)

        # Invalid orders must not enter the buffer.
        assert get_order(memory) is None
    finally:
        memory.close()


def test_invalid_order_preserves_existing_order():
    memory = create_memory()
    try:
        put_order(memory, 1001, 15606, 7, 0)

        with pytest.raises(ValueError):
            put_order(memory, 1002, 15606, -5, 0)

        # The invalid order must not overwrite the valid order.
        assert get_order(memory) == (1001, 15606, 7, 0)
        assert get_order(memory) is None
    finally:
        memory.close()



def test_open_shared_memory_missing_file(tmp_path):
    from src.ipc.shared_ring_buffer import open_shared_memory

    missing_file = tmp_path / "missing.dat"

    with pytest.raises(FileNotFoundError):
        open_shared_memory(str(missing_file))
def test_locked_put_and_get():
    memory = create_memory()
    lock = Lock()

    try:
        put_order_locked(lock, memory, 2002, 15025, 10, 0)
        assert get_order_locked(lock, memory) == (2002, 15025, 10, 0)
        assert get_order_locked(lock, memory) is None
    finally:
        memory.close()

def test_create_shared_memory(tmp_path):
    from src.ipc.shared_ring_buffer import (
        create_shared_memory,
        get_indices,
        BUFFER_SIZE,
    )

    filename = str(tmp_path / "shared_orders.dat")

    file_handle, memory = create_shared_memory(filename)

    try:
        assert len(memory) == BUFFER_SIZE
        assert get_indices(memory) == (0, 0)
    finally:
        memory.close()
        file_handle.close()
def test_file_backed_producer_consumer(tmp_path):
    from src.ipc.shared_ring_buffer import (
        create_shared_memory,
        open_shared_memory,
    )

    filename = str(tmp_path / "integration_orders.dat")

    # Producer creates the shared-memory file and writes an order.
    producer_file, producer_memory = create_shared_memory(filename)
    try:
        put_order(producer_memory, 2002, 15025, 10, 0)
        producer_memory.flush()
    finally:
        producer_memory.close()
        producer_file.close()

    # Consumer opens the same file and reads the order.
    consumer_file, consumer_memory = open_shared_memory(filename)
    try:
        assert get_order(consumer_memory) == (2002, 15025, 10, 0)
        assert get_order(consumer_memory) is None
    finally:
        consumer_memory.close()
        consumer_file.close()