# ChronosMatch

Zero-copy shared-memory order pipeline for a high-frequency trading engine.

ChronosMatch connects an asynchronous market simulator to a matching engine using a binary shared-memory ring buffer. It supports cross-process order delivery, trade execution, performance measurement, and a terminal dashboard.

## Architecture

```text
Market Simulator (Producer)
          |
          v
   Binary IPC Buffer
      mmap + struct
          |
          v
 Matching Engine (Consumer)
          |
          v
       OrderBook
          |
          v
 Performance Metrics
          |
          v
   Terminal Dashboard
```

## Technology Stack

- Python
- asyncio
- multiprocessing
- mmap
- struct
- pytest

## Binary IPC Design

Each order occupies 16 bytes using the `!IIII` format.

| Field | Type | Description |
|---|---|---|
| order_id | uint32 | Unique order identifier |
| price_ticks | uint32 | Price in integer ticks |
| quantity | uint32 | Order quantity |
| side | uint32 | 0 = BUY, 1 = SELL |

Price representation: 100 ticks = 1 currency unit. For example, 150.25 = 15025 ticks.

The ring buffer contains an 8-byte header and 1,024 slots, with 1,023 usable slots to distinguish full and empty states.

Implementation: `src/ipc/shared_ring_buffer.py`

## Matching Engine

The consumer passes received orders to `src/engine/order_book.py`.

- BUY side is encoded as 0.
- SELL side is encoded as 1.
- Orders match when prices cross or equal the opposite best price.
- Trades execute at the resting order price.
- Unfilled quantities remain in the order book.

## Multi-Process Integration

The producer and consumer communicate through binary shared memory.

The integration includes:

1. Asynchronous market order generation.
2. Binary serialization using `!IIII`.
3. Cross-process communication using mmap.
4. Consumer-side order decoding.
5. Matching through `OrderBook.add_order()`.
6. FIFO sequence validation.
7. Trade statistics and performance measurement.
8. Process coordination and resource cleanup.

Implementation: `src/ipc/multiprocess_runner.py`

## Performance Dashboard

The Windows-compatible terminal dashboard displays:

- Orders processed.
- Average matching-engine processing latency.
- Matching throughput.

Matching-engine processing latency is different from end-to-end pipeline latency.

## Running the Application

Run commands from the project root.

Display available options:

```powershell
python app.py --help
```

Run a 1,000-order multi-process simulation:

```powershell
python app.py --orders 1000 --ipc binary --multiprocess --verbose
```

Run a 10,000-order simulation:

```powershell
python app.py --orders 10000 --ipc binary --multiprocess
```

Run the IPC demo:

```powershell
python run_ipc.py
```

## Running Tests

Run all tests:

```powershell
python -m pytest -v
```

Run the multiprocess matching test:

```powershell
python -m pytest tests/test_multiprocess_matching.py -v
```

## Performance Goal

The project targets low-latency order processing, with a design goal of under 50 microseconds.

Actual performance depends on hardware and execution conditions. Single-run measurements do not guarantee consistent latency or throughput.

## Project Status

The Python multi-process integration has been implemented and tested.

The integrated application includes:

- Market simulation.
- Binary shared-memory IPC.
- Matching-engine order processing.
- FIFO validation.
- Trade statistics.
- Performance dashboard.
