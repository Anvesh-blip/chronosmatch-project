import mmap
import pytest

from src.ipc.shared_ring_buffer import (
    BUFFER_SIZE,
    CAPACITY,
    initialize_header,
    put_order,
    get_order,
)


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