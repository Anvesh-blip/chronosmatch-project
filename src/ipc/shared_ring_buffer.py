import mmap
import os
import struct

# Binary format: order_id, price_ticks, quantity, side
ORDER_FORMAT = "!IIII"

# Each order occupies 16 bytes
ORDER_SIZE = struct.calcsize(ORDER_FORMAT)

# Ring buffer configuration
CAPACITY = 1024
HEADER_SIZE = 8
BUFFER_SIZE = HEADER_SIZE + (CAPACITY * ORDER_SIZE)

def initialize_header(memory):
    struct.pack_into("!II", memory, 0, 0, 0)

def get_indices(memory):
    return struct.unpack_from("!II", memory, 0)

def set_indices(memory, read_index, write_index):
    struct.pack_into("!II", memory, 0, read_index, write_index)

def next_index(index):
    return (index + 1) % CAPACITY

def write_order(memory, slot, order_id, price_ticks, quantity, side):
    offset = HEADER_SIZE + (slot * ORDER_SIZE)

    struct.pack_into(
        ORDER_FORMAT,
        memory,
        offset,
        order_id,
        price_ticks,
        quantity,
        side,
    )

def read_order(memory, slot):
    offset = HEADER_SIZE + (slot * ORDER_SIZE)
    return struct.unpack_from(ORDER_FORMAT, memory, offset)

def validate_order(order_id, price_ticks, quantity, side):
    max_value = (1 << 32) - 1

    for name, value in [
        ("order_id", order_id),
        ("price_ticks", price_ticks),
        ("quantity", quantity),
        ("side", side),
    ]:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"{name} must be an integer")

        if not 0 <= value <= max_value:
            raise ValueError(f"{name} must be between 0 and {max_value}")

    if price_ticks == 0:
        raise ValueError("price_ticks must be greater than zero")

    if quantity == 0:
        raise ValueError("quantity must be greater than zero")

    if side not in (0, 1):
        raise ValueError("side must be 0 (BUY) or 1 (SELL)")


def put_order(memory, order_id, price_ticks, quantity, side):
    validate_order(order_id, price_ticks, quantity, side)
    read_index, write_index = get_indices(memory)
    next_write = next_index(write_index)

    if next_write == read_index:
        raise BufferError("Ring buffer is full")

    write_order(
        memory,
        write_index,
        order_id,
        price_ticks,
        quantity,
        side,
    )

    set_indices(memory, read_index, next_write)

def get_order(memory):
    read_index, write_index = get_indices(memory)

    if read_index == write_index:
        return None

    order = read_order(memory, read_index)
    next_read = next_index(read_index)

    set_indices(memory, next_read, write_index)

    return order

def open_shared_memory(filename="shared_orders.dat"):
    file_handle = open(filename, "r+b")

    try:
        memory = mmap.mmap(file_handle.fileno(), BUFFER_SIZE)
        return file_handle, memory
    except Exception:
        file_handle.close()
        raise


def put_order_locked(lock, memory, order_id, price_ticks, quantity, side):
    with lock:
        put_order(memory, order_id, price_ticks, quantity, side)


def get_order_locked(lock, memory):
    with lock:
        return get_order(memory)

