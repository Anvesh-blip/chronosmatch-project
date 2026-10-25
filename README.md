# ChronosMatch

Zero-copy, shared-memory order pipeline for a high-frequency trading engine.

## IPC Module

The IPC module uses a file-backed memory map (`mmap`) and Python's `struct` module to exchange fixed-size binary orders between producer and consumer processes.

### Binary Order Format

Each order occupies **16 bytes**, using the `!IIII` format:

| Field | Type | Description |
|---|---|---|
| `order_id` | `uint32` | Unique order identifier |
| `price_ticks` | `uint32` | Price in ticks; 100 ticks = 1 currency unit |
| `quantity` | `uint32` | Order quantity |
| `side` | `uint32` | `0` = BUY, `1` = SELL |

The buffer uses an 8-byte header containing two unsigned 32-bit indexes:

- `read_index`: next order to consume
- `write_index`: next slot to write

The ring buffer has 1,024 slots, with 1,023 usable slots to distinguish full and empty states.

### Python API

The shared-memory implementation is located at `src/ipc/shared_ring_buffer.py`.

- `create_shared_memory()` — creates and initializes the backing file.
- `open_shared_memory()` — opens an existing backing file.
- `put_order_locked()` — validates and writes an order while holding a shared lock.
- `get_order_locked()` — reads an order while holding a shared lock.

**Important:** Initialize the backing file only once before starting the producer and consumer. Both processes must use the same file and the same shared lock.

### Running the IPC Demo

```powershell
python -u run_ipc.py
```

### Running Tests

```powershell
python -m pytest -v
```

### Integration Notes

The matching engine must agree with the IPC module on:

- The 8-byte header layout
- The 16-byte `!IIII` order layout
- Price representation in integer ticks
- BUY/SELL encoding
- Ring-buffer index and full/empty conventions

The current Python implementation uses a shared multiprocessing lock. A native Cython consumer will need compatible synchronization before accessing the buffer concurrently.
