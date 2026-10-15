# ChronosMatch — Zero-Copy High-Frequency Trading Engine

Educational trading-engine prototype demonstrating low-latency order ingestion, shared-memory inter-process communication (IPC), Cython-accelerated order book matching, and terminal performance monitoring.

---

## Architecture Overview

```text
1. Market Simulator (Arjit)
   [asyncio synthetic BUY/SELL order generation & rate pacing]
         |
         | (1) In-Memory RingBuffer [Prototype]
         | (2) File-Backed mmap RingBuffer (16-byte binary struct) [Binary IPC]
         v rb.put(order)
2. Shared-Memory IPC RingBuffer (Anvesh / Arjit)
   [circular buffer with backpressure, multiprocessing.Lock & BufferError signaling]
         |
         v rb.get()
3. Limit Order Book & Matching Engine (PranKurshuk)
   [price-time priority FIFO matching & Cython acceleration]
         |
         +--> 4. Terminal Dashboard & Metrics (Yuvika)
         |    [curses UI, latency telemetry, audit logs]
         v
      SQLite Trade Persistence
```

---

## Binary IPC Specification (Proposed Design)

### Memory-Mapped File Geometry
- **Backing File:** File-backed `mmap` shared between separate producer and consumer processes.
- **Process Startup Architecture (Windows):** Python 3.11 with `spawn` multiprocessing start method.
- **Header:** 8 bytes total at offset 0 (`struct.Struct("!II")`):
  - Offset 0–3: `read_index` (32-bit unsigned big-endian integer)
  - Offset 4–7: `write_index` (32-bit unsigned big-endian integer)
- **Ring Buffer Slots:** 1024 total slots (`TOTAL_SLOTS = 1024`).
- **Usable Orders:** 1023 orders maximum before raising `BufferError` (preserves single-slot empty/full distinction).
- **Slot Size:** 16 bytes per order record.
- **Total Mapping Size:** `8 + (1024 * 16) = 16,392 bytes`.

### Binary Order Record Layout (`struct.Struct("!IIII")`)
Each order is packed into exactly 16 bytes:

| Field | Format | Offset | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | `uint32` (`I`) | Byte 0–3 | Unique sequential positive integer (`0` to `4,294,967,295`) |
| `price_ticks` | `uint32` (`I`) | Byte 4–7 | Price scaled to 100 ticks per rupee (`int(round(price * 100))`) |
| `quantity` | `uint32` (`I`) | Byte 8–11 | Positive integer quantity |
| `side` | `uint32` (`I`) | Byte 12–15 | Side indicator: `0 = BUY`, `1 = SELL` |

### Synchronization & Safety Guarantees
- **Parent Initialization:** The parent process creates/sizes the 16,392-byte file and sets both indices to zero (`read_index = 0`, `write_index = 0`) before spawning child processes.
- **Shared Lock Passing:** The parent creates one `multiprocessing.Lock` (via `ctx.Lock()`) and passes the same lock object to both child processes as an argument.
- **Ordering Guarantee:**
  - **Producer:** Writes the complete 16-byte order record into the target slot *before* publishing the updated `write_index` in the shared header.
  - **Consumer:** Reads the complete 16-byte order record from the slot *before* publishing the updated `read_index` in the shared header.
- **Safe Cleanup:** Child processes cleanly close their `mmap` and file handles. The parent process waits for children to terminate (`join()`) before unmapping and unlinking the temporary backing file, avoiding Windows file-lock errors (`WinError 32`).

---

## Quick Start & Demo Commands

### Prerequisites

- Python 3.10+
- `pytest` and `pytest-asyncio`

```bash
pip install pytest pytest-asyncio
```

### 1. Cross-Process Binary IPC (Windows `spawn` Multiprocessing)

Spawns separate producer and consumer processes communicating via 16-byte binary records over the shared mmap ring buffer:

```bash
python market_simulator.py --orders 1000 --multiprocess
```

Streaming 2,500 orders (exceeds buffer capacity, testing concurrent wrap-around and backpressure):

```bash
python market_simulator.py --orders 2500 --multiprocess
```

### 2. Single-Process Binary IPC (`--ipc binary`)

Runs the asynchronous simulator with the file-backed binary mmap ring buffer in a single process with a background consumer task:

```bash
python market_simulator.py --orders 1000 --ipc binary --with-consumer
```

### 3. In-Memory Prototype Simulation (`--ipc list`)

Runs Anvesh's list-based prototype ring buffer:

```bash
python market_simulator.py --orders 1000 --ipc list --with-consumer
```

### 4. Backpressure & Retry Demonstration

Constrain the buffer capacity to observe retry attempts and failure policies without a consumer:

```bash
# Continue policy (counts failed orders after max-retries):
python market_simulator.py --orders 50 --buffer-capacity 10 --max-retries 3 --retry-delay 0.002 --on-exhausted continue

# Stop policy (halts simulation upon first exhausted order):
python market_simulator.py --orders 50 --buffer-capacity 10 --max-retries 2 --on-exhausted stop
```

---

## Running Tests

Run the complete test suite (39 tests covering OrderGenerator, Codec, BinaryRingBuffer, and Cross-Process IPC):

```bash
python -m pytest tests/ -v
```

Run Anvesh's baseline ring buffer test:

```bash
python test_ring_buffer.py
```

---

## Sample Multi-Process Simulation Report

*Captured on local Windows development machine using Python 3.11 `spawn` start method:*

```text
=================================================================
 CHRONOSMATCH - Zero-Copy High-Frequency Trading Engine
 Module: Async Market Simulator (Engineer: Arjit)
 IPC Mode: BINARY (Cross-Process Spawn)
=================================================================
 Launching cross-process simulation (Windows spawn method)...
 Producer: MarketSimulator process | Consumer: Matching engine reader process
=================================================================

=================================================================
 CHRONOSMATCH MULTI-PROCESS SIMULATOR - PERFORMANCE REPORT
 Mode: Cross-Process Binary IPC (Windows spawn + 16-byte struct)
=================================================================
 Total orders generated:                  1,000
 Total orders submitted (success):        1,000
 Total orders failed:                         0
-----------------------------------------------------------------
 Buffer-full events (BufferError):            0
 Retry attempts made:                         0
 Retries exhausted events:                    0
-----------------------------------------------------------------
 Total execution time:                   0.0264 s
 Actual submitted orders / sec:        37941.16 ops/s
-----------------------------------------------------------------
 Average submission latency:             16.765 us (16,765.4 ns)
-----------------------------------------------------------------
 Consumer drained orders:                 1,000
 First order ID received:          1001
 Last order ID received:           2000
=================================================================
```

---

## CLI Configuration Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--orders` | int | `1000` | Total number of orders to generate |
| `--rate` | float | `None` | Submission rate in orders/second (`None` = unthrottled) |
| `--ipc` | str | `list` | Buffer implementation: `list` (prototype) or `binary` (mmap struct) |
| `--multiprocess` | flag | `False` | Run cross-process simulation using Windows `spawn` |
| `--buffer-capacity`| int | `2048` | Capacity for in-memory RingBuffer (`>= 2`) |
| `--seed` | int | `None` | RNG seed for deterministic order sequences |
| `--max-retries` | int | `5` | Maximum retries on `BufferError` before failing order |
| `--retry-delay` | float | `0.001`| Backpressure delay in seconds between retries |
| `--on-exhausted` | str | `continue` | Action when retries exhaust: `continue` or `stop` |
| `--with-consumer` | flag | `False` | Run concurrent background draining task |
| `--verbose`, `-v` | flag | `False` | Enable DEBUG level logging |

---

## Team Integration & Confirmation Points for Anvesh (IPC Engineer)

1. **Confirmation of 16-Byte Struct Layout:**
   - Confirm layout: `struct.Struct("!IIII")` (Big-endian: `order_id` (4B), `price_ticks` (4B), `quantity` (4B), `side` (4B)).
2. **Fixed-Point Price Scaling:**
   - Confirm 100 ticks per rupee (`TICKS_PER_RUPEE = 100`, e.g. ₹150.25 -> 15025 ticks).
3. **Timestamp Metadata:**
   - If Yuvika or PranKurshuk require order timestamping in the binary header, confirm if struct should expand to 24 bytes (e.g. `!IIIIQ` with 64-bit nanosecond timestamp).
4. **Locking Mechanism:**
   - Confirmed: single `multiprocessing.Lock` created by parent and passed to spawned child processes.
5. **Backwards Compatibility:**
   - Anvesh's list-based prototype `src/ipc/ring_buffer.py` remains 100% untouched and operational for any module still using the initial API.
