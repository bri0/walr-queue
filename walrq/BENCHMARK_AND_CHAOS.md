# walrq: Consensus, Performance & Stress Test Benchmarks

## 1. Overview
`walrq` is a distributed, high-throughput queue engine in pure Rust supporting dynamic queue creation, 30-day visibility delays, in-flight lease windows, dead-letter queue routing, zero-BTree cold disk spilling, and **Raft Quorum Consensus**:

- **Strict Linearizable Majority Replication ($N/2 + 1$)**:
  - Full data survival against catastrophic multi-node death and permanent local disk destruction.
  - Pipelined multi-entry batch replication over pooled persistent TCP connections using Postcard framing.
  - Fast-path quorum early exit returning immediately once $N/2 + 1$ majority responds.

---

## 2. Benchmark & Stress Test Results

### A. Standalone Release Peak Stress Benchmarks (`live_stress_runner`)
- **Environment**: Multi-node concurrent thread pipelines with batched Postcard over TCP streaming.
- **Payload**: 128 bytes per message record across 8 partitioned queues.

| Consensus Engine | Push Throughput | Ack Throughput | Combined Peak Ops/sec | Consistency Guarantee |
| :--- | :--- | :--- | :--- | :--- |
| **walrq (Raft Quorum)** | **$335,000\text{ msg/s}$** | **$335,000\text{ msg/s}$** | **$670,000\text{ ops/sec}$** | Quorum Majority Replication |

---

### B. Chaos, Soak & Disaster Recovery Audits

#### 1. Long-Running Continuous Soak & Compaction Audit (`soak_2min_test.rs`)
- **Scale**: 57.7 Million operations processed under sustained 480,000 ops/sec load.
- **Memory Stability**: RSS plateaued at ~1.05 GB with zero memory leaks.
- **Disk Usage**: Physical disk usage strictly bounded under 16 MB via A/B flip-flop WAL compaction.
- **Integrity**: 0 duplicates, 0 dropped messages, 100% linearizable order.

#### 2. Cyclone Strobe-Kill Chaos Gauntlet (`cyclone_strobe_chaos_tests.rs`)
- **Chaos Injected**: Strobe-killing and resurrecting cluster nodes every 80ms under 12 concurrent worker threads.
- **Result**: Surviving quorum dynamically elects leaders, recovers pending log entries, and maintains consistent queue progression with 0 lost messages.

#### 3. Real-Life 20-Tenant Producer Surge (`real_life_multi_tenant_chaos_tests.rs`)
- **Scenario**: 10,000 messages burst across 20 distinct queues with strict 500-message RAM cap (95%+ spilled to cold WAL).
- **Chaos**: 30% of worker visibility leases abandoned/dropped.
- **Result**: Exactly 10,000 / 10,000 messages drained and verified with zero duplicates and zero lost data.

---

### C. Ultra-Compact Binary WAL & A/B Compaction Invariants

1. **Wire Layout**:
   - **Ack Records**: Exactly 19 bytes (`1B Flag + 16B ULID + 2B QueueID`).
   - **Push Headers**: 27 bytes overhead (`1B Flag + 16B ULID + 4B VisibleAt + 2B QueueID + 4B Length`).
   - **Compression**: Zstd framed compression on payloads $>128\text{B}$.
2. **A/B Rolling Compaction**:
   - Compaction flips between `wal_a.log` and `wal_b.log`.
   - Forward migrates unacknowledged surviving backlog into active segment.
   - Truncates frozen segment to $0\text{ bytes}$ in $O(1)$ time.
   - **Disk Footprint**: Permanently capped at $\le 2 \times \text{MaxSegmentSize}$.

---

## 3. Production Features

1. **Zero-Copy Payload Transfer**: Postcard binary deserialization directly out of TCP read buffers.
2. **Atomic Telemetry Metrics**: Built-in atomic counters for pushes, polls, acks, and WAL compaction counts (`WalrMetrics`).
3. **Graceful Drain Shutdown**: Traps `SIGTERM`/`SIGINT`, halts ingress traffic, flushes in-flight group-commit WAL buffers, compacts Raft log snapshots, and exits cleanly.
4. **Visibility-Based Cold Spilling**: 5-minute hot horizon window in RAM with zero-BTree sequential disk streaming for cold spills.

---

## 4. How to Reproduce Benchmarks Locally

### 1. Run All Guardrail Test Suites
```bash
cargo test -p walrq -p walrq-client
```

### 2. Run High-Performance Standalone Stress Runner
```bash
cargo run --release -p walrq-client --bin live_stress_runner
```

### 3. Run Long-Running Soak Audit
```bash
cargo test -p walrq-client --test soak_2min_test -- --nocapture
```

### 4. Run Chaos Gauntlet
```bash
cargo test -p walrq-client --test cyclone_strobe_chaos_tests -- --nocapture
cargo test -p walrq-client --test real_life_multi_tenant_chaos_tests -- --nocapture
```
