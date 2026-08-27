# Parallel Firewall

A multi-threaded packet filter written in C. Packets are ingested by a producer thread, queued in a bounded ring buffer, and classified in parallel by a pool of consumer threads. Decisions are written to a log in arrival order.

The serial binary is the reference implementation: same filter rules, single-threaded, same log format.

## How it works

```
capture file ──► producer ──► ring buffer ──► consumer pool ──► decision log
                                      │
                                      └── mutex + condition variables
                                          (no busy-waiting)
```

1. **Producer** reads fixed-size packets from an input file and enqueues them.
2. **Ring buffer** is a circular byte buffer shared across threads. Producers block when it is full; consumers block when it is empty. `ring_buffer_stop()` wakes remaining consumers so they can exit.
3. **Consumers** dequeue packets, run the filter (`PASS` / `DROP`), hash the packet, and append a log line. Writes are sequenced so the log matches producer order even though classification runs concurrently.

Each packet is 256 bytes: source, destination, timestamp, and payload. The filter allows a packet when its source address falls in one of the configured ranges.

Log line format (same as the serial path):

```
PASS|DROP  <16-digit hex hash>  <timestamp>
```

## Build

Requires a C compiler and POSIX threads (`pthread`).

```bash
make -C src
```

This produces `src/firewall` (parallel) and `src/serial` (reference).

```bash
make -C src clean
```

## Usage

```bash
./src/firewall <input-file> <output-file> <num-consumers>
```

`num-consumers` must be between 1 and 32.

```bash
./src/serial <input-file> <output-file>
```

Generate a capture file, then run the parallel filter:

```bash
python3 tests/gen_packets.py generate packets.in 1000
./src/firewall packets.in decisions.log 4
```

## Tests

The test harness builds both binaries, generates captures of 10 to 20,000 packets, and compares the parallel log against the serial reference. Parallel runs are repeated to catch races. A subset of tests also checks that the log is written in order while the program is still running.

```bash
cd tests
./grade.sh
```

Or run the checker only:

```bash
cd tests
make check
```

Remove generated inputs, outputs, and object files:

```bash
make -C tests distclean
```

## Project layout

| Path | Role |
|------|------|
| `src/firewall.c` | Parallel entry point |
| `src/serial.c` | Single-threaded reference |
| `src/producer.c` | Reads packets and fills the ring buffer |
| `src/consumer.c` | Worker pool: classify, hash, ordered log |
| `src/ring_buffer.c` | Thread-safe circular buffer |
| `src/packet.c` | Filter rules and packet hash |
| `utils/` | Logging helpers |
| `tests/` | Packet generator and checker |

## License

BSD-3-Clause. See the SPDX headers in the source files.
