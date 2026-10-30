# ChronosMatch

Zero-copy, shared-memory order pipeline for a high-frequency trading engine.

## IPC Module

The IPC module uses a file-backed memory map (`mmap`) and Python's `struct` module to exchange fixed-size binary orders between producer and consumer processes.

### Binary Order Format

Each order occupies **16 bytes**, using the `!IIII` format.

| Field | Type | Description |
|---|---|---|
| `order_id` | `uint32` | Unique order identifier |
| `price_ticks` | `uint32` | Price in ticks; 100 ticks = 1 currency unit |
| `quantity` | `uint32` | Order quantity |
| `side` | `uint32` | `0` = BUY, `1` = SELL |

The buffer uses an 8-byte header containing two unsigned 32-bit indexes:

- `read_index`: Next order to consume.
- `write_index`: Next slot to write.

The ring buffer has 1,024 slots, with 1,023 usable slots to distinguish full and empty states.

### Python API

The current shared-memory implementation is located at `src/ipc/shared_ring_buffer.py`.

- `create_shared_memory()` — Creates and initializes the backing file.
- `open_shared_memory()` — Opens an existing backing file.
- `put_order_locked()` — Validates and writes an order while holding a shared lock.
- `get_order_locked()` — Reads an order while holding a shared lock.

**Important:** Initialize the backing file only once before starting the producer and consumer. Both processes must use the same backing file and the same shared multiprocessing lock.

### Running the IPC Demo

```powershell
python -u run_ipc.py
```

### Running Tests

```powershell
python -m pytest -v
```

### Integration Notes

The matching engine and IPC module must agree on:

- The 8-byte header layout.
- The 16-byte `!IIII` order layout.
- Price representation in integer ticks.
- BUY/SELL encoding.
- Ring-buffer index and full/empty conventions.

The current Python implementation uses a shared multiprocessing lock. A native Cython consumer must use compatible synchronization before accessing the buffer concurrently.

## Current Matching Engine Integration

The current IPC consumer passes each received order to `src/engine/order_book.py`.

- BUY side is `0`; SELL side is `1`.
- Orders match when their prices cross or equal the opposite best price.
- Trades execute at the resting order price.
- Unfilled quantities remain in the order book.
- Order ID `0` is reserved as the stop signal in the current IPC demo.

Run the integrated IPC and matching-engine demo:

```powershell
python run_ipc.py
```

Run the tests:

```powershell
python -m pytest -v
```

## Planned Multi-Process Integration with Market Simulator

The Market Simulator and IPC modules will be connected through the shared binary ring buffer.

- The producer generates BUY and SELL orders asynchronously.
- Orders are encoded using the agreed `!IIII` binary format.
- The consumer reads orders from the shared memory buffer.
- Each order is validated and converted into the matching engine's expected format.
- The consumer passes valid orders to `OrderBook.add_order()`.
- Executed trades and remaining quantities are handled by the matching engine.
- Producer completion and buffer-drained status are coordinated before consumer shutdown.

### Final Integration Tests

1. Verify BUY and SELL order delivery and trade execution.
2. Verify buffer-full handling and retry behavior.
3. Verify shared-lock synchronization between producer and consumer.
4. Test invalid orders and corrupted records.
5. Verify clean shutdown and resource cleanup.

**Note:** The multi-process integration is planned and must pass end-to-end validation before it is considered complete.