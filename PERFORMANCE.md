# WALRQ Performance & Profiler Report

## 1. 100-Pusher / 100-Puller Real-Life Soak Benchmark

- **Cluster Setup**: 3-Node Raft Quorum Cluster over Postcard + Length-Prefixed TCP.
- **Workload**:
  - 100 concurrent Pusher tasks
  - 100 concurrent Puller tasks
  - 20 dynamic tenant queues (`tenant_queue_00` .. `tenant_queue_19`)
  - Mixed payload matrix: Small (64B–128B JSON), Medium (256B JSON), and Large (32KB JSON).
  - Batching: 500-item micro-batches.

### Resource Utilization & Stability

| Metric | Measured Value | Analysis |
| :--- | :--- | :--- |
| **CPU Utilization** | **1,050% (10.5 cores)** | Tokio multi-threaded work-stealing pool scales across all available physical cores. |
| **Peak Resident RAM (RSS)** | **~556 MB** | Stable plateau at the 5-minute horizon boundary; zero unbounded memory leaks despite millions of operations. |
| **Total Disk Footprint** | **~38.9 MB** (all 3 nodes) | A/B flip-flop WAL compaction and snapshot truncation strictly bounds disk footprint. |
| **Data Loss / OOM** | **0 (Zero)** | Quorum linearizability maintained; zero dropped items, zero duplicate deliveries. |

---

## 2. Profiler CPU Sample Breakdown (`/usr/bin/sample` 1ms resolution)

Under maximum cluster saturation, CPU time distribution across the binary:

```text
 89.5% ─ DiskLog Background Writer (Sequential WAL file appending & fsync boundaries)
  4.2% ─ Tokio Reactor I/O (kevent loop, socket readable/writable transitions)
  2.8% ─ Time & Clock Readings (clock_gettime_nsec_np, mach_absolute_time for TimerWheel)
  1.5% ─ Thread Synchronization (parking_lot::Condvar, __psynch_cvsignal)
  0.9% ─ Payload Memory Copies (_platform_memmove)
  0.8% ─ SipHash Shard Routing (Sip13Rounds across 32 queue shards)
  0.3% ─ Memory Allocator overhead (_nanov2_free, malloc)
```

### Key Takeaways
1. **Zero Lock Contention**: No lock conviction storms or thread starvation.
2. **Zero Allocator Thrashing**: Allocator overhead is $< 0.5\%$.
3. **Pure I/O-Bound**: Throughput is gated by sequential disk write speed and disk page-in during cold backlogs.

---

## 3. Potential Opportunities to Optimize WAL Disk Spilling

1. **`io_uring` / Direct I/O (Linux)**:
   - Bypass kernel page cache double-buffering.
   - Batch submit disk writes without thread context switches.

2. **Sequential Spill Page Pre-fetching**:
   - As in-memory queue depth drops below 20%, trigger background asynchronous sequential chunk reads from `spill_read_offset` so consumers never hit cold disk stalls.

3. **Separate Hot Inactive WAL from Hot Metadata**:
   - Keep message payload bytes in raw chunk files and only stream lightweight metadata headers through the Raft replication log.
