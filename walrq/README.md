# walrq

**`walrq`** is the core server engine crate for the `walrq` distributed queue system.

For full architectural documentation, design decisions, and benchmarks, see the master [Repository README](../README.md).

---

## Architecture Summary
- **Binary WAL**: Append-only binary log with 2-byte dictionary queue encoding, 19-byte ACK records, and 27-byte push headers.
- **Raft Quorum Consensus**: Synchronous majority replication ($N/2 + 1$), fast-path early quorum return, and compacted snapshot log catchup.
- **Visibility-Based Disk Spilling**: 5-minute hot horizon window (1-minute resolution buckets in RAM) with zero-BTree sequential disk streaming for cold spills.
- **Micro-Batching TCP Server**: Postcard serialization over 4-byte length-prefixed persistent TCP connections.

---

## Running Standalone

```bash
# Build
cargo build --release -p walrq

# Start single node
DATA_DIR="./data/node1" NODE_ID="127.0.0.1:50051" cargo run --release -p walrq
```
