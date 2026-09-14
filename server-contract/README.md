# Walrq Server Wire Contract

This document defines the wire specification for Walrq. Any SDK (Go, Python, TypeScript, Java, C#, C/C++) can communicate with a Walrq server by following these framing and serialization rules.

---

## 1. TCP Transport & Connection Model

- **Transport**: Standard persistent TCP connection.
- **Port**: Default `50051`.
- **Multiplexing**: Requests are synchronous/sequential per TCP connection: send one request frame, read one response frame. Open multiple TCP connections for concurrent throughput.
- **Byte Order**: Network byte order (**Big-Endian**) for length framing.

---

## 2. Stream Framing

Every message sent over TCP (both request and response) uses a 4-byte length prefix followed by the payload:

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Payload Length (u32, BE)                  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                   Postcard Encoded Payload                    |
|                        (N bytes)                              |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

1. **Payload Length** (`4 bytes`, unsigned 32-bit integer, Big-Endian): number of bytes in the subsequent postcard body.
2. **Postcard Encoded Payload** (`N bytes`): The serialized payload.

---

## 3. Serialization: Postcard Specification

Postcard is an open, compact binary serialization format based on Serde:
- **Integers**: Encoded using **LEB128 Varints** (little-endian base 128). Unsigned integers (`u32`, `u64`, `usize`) use standard unsigned LEB128.
- **Strings**: Encoded as length (LEB128 `usize`) followed by UTF-8 bytes.
- **Byte Arrays (`Vec<u8>`)**: Encoded as length (LEB128 `usize`) followed by raw bytes.
- **Vectors / Arrays**: Encoded as element count (LEB128 `usize`) followed by each serialized element sequentially.
- **Booleans**: Single byte (`0x00` = false, `0x01` = true).
- **Enums**: Encoded as variant index (LEB128 `u32`), starting from `0`, followed by the variant fields in order.

---

## 4. Message Definitions

### 4.1 Client Request (`Request` Enum)

The outer message is a Rust enum with variant index:

| Variant Index | Name | Fields | Description |
|---|---|---|---|
| `0` | **`Push`** | `queue_name: String`<br>`payload: Vec<u8>`<br>`delay_seconds: u64`<br>`message_id: String` | Push single message. If `message_id` is empty (`""`), server generates ULID. |
| `1` | **`PushBatch`** | `queue_name: String`<br>`items: Vec<BatchPushItem>` | Push batch of messages. |
| `2` | **`Poll`** | `queue_name: String`<br>`visibility_timeout_sec: u32`<br>`batch_size: u32` | Poll available messages. |
| `3` | **`Ack`** | `queue_name: String`<br>`message_id: String`<br>`receipt_handle: String` | Acknowledge single message. |
| `4` | **`AckBatch`** | `queue_name: String`<br>`items: Vec<AckItem>` | Acknowledge batch of messages. |
| `5` | **`RaftVote`** | *(Internal Raft Peer RPC)* | term (u64), candidate_id (String), last_log_index (u64), last_log_term (u64) |
| `6` | **`RaftAppend`** | *(Internal Raft Peer RPC)* | term (u64), leader_id (String), prev_log_index (u64), prev_log_term (u64), entries, leader_commit (u64) |

#### Inner Struct: `BatchPushItem`
Sequential fields:
1. `payload: Vec<u8>` (LEB128 len + bytes)
2. `delay_seconds: u64` (LEB128 varint)
3. `message_id: String` (LEB128 len + UTF-8 string; empty string `""` to auto-generate ULID)

#### Inner Struct: `AckItem`
Sequential fields:
1. `message_id: String` (LEB128 len + UTF-8 string)
2. `receipt_handle: String` (LEB128 len + UTF-8 string)

---

### 4.2 Server Response (`Response` Enum)

The response is a Rust enum with variant index:

| Variant Index | Name | Fields | Description |
|---|---|---|---|
| `0` | **`Push`** | `message_id: String` | Returns assigned/persisted message ID (ULID). |
| `1` | **`PushBatch`** | `message_ids: Vec<String>` | Returns array of assigned message IDs. |
| `2` | **`Poll`** | `messages: Vec<Message>` | List of polled messages. |
| `3` | **`Ack`** | `success: bool` | True if deleted/acknowledged. |
| `4` | **`AckBatch`** | `acked_count: u32` | Number of messages acknowledged. |
| `5` | **`RaftVote`** | `term: u64`, `vote_granted: bool` | Peer Raft vote result. |
| `6` | **`RaftAppend`** | `term: u64`, `success: bool` | Peer Raft append result. |
| `7` | **`Redirect`** | `leader: String` | Node is follower. Redirect target is `host:port`. |
| `8` | **`Error`** | `message: String` | Request error description. |

#### Inner Struct: `Message`
Returned inside `Response::Poll`:
1. `message_id: String` (LEB128 len + UTF-8 string)
2. `payload: Vec<u8>` (LEB128 len + bytes)
3. `receipt_handle: String` (LEB128 len + UTF-8 string)
4. `delivery_count: u32` (LEB128 varint)

---

## 5. Cluster Routing & Leader Redirect Protocol

Walrq uses Raft quorum. Writes and reads must route to the active cluster Leader:

1. When client sends any request to a node that is **not** the active leader, the node responds with:
   - Variant `7` (`Response::Redirect`)
   - Field `leader: String` containing target `"ip:port"` or `"host:port"` (e.g. `"10.0.1.15:50051"`).
2. The SDK must:
   - Cache this target for subsequent calls to that queue.
   - Close current socket or keep connection open in a connection pool.
   - Resend the pending request to the new leader address.

---

## 6. Pseudocode Example for Other SDKs (e.g., Python / Go)

### Encoding `Request::Push`:
```python
# 1. Variant Index 0 (LEB128 for 0 -> 0x00)
buf.write_u8(0)

# 2. queue_name: "orders" (len 6 as LEB128 -> 0x06 + utf8)
buf.write_varint(len("orders"))
buf.write(b"orders")

# 3. payload: b"hello" (len 5 as LEB128 -> 0x05 + raw bytes)
buf.write_varint(len(payload))
buf.write(payload)

# 4. delay_seconds: 0 (LEB128 -> 0x00)
buf.write_varint(0)

# 5. message_id: "" (empty string: len 0 -> 0x00)
buf.write_varint(0)

# Send over socket:
frame_len = len(buf)
tcp_socket.sendall(struct.pack(">I", frame_len) + buf.getvalue())
```

### Reading Response:
```python
# 1. Read 4-byte length prefix
raw_len = tcp_socket.recv(4)
frame_len = struct.unpack(">I", raw_len)[0]

# 2. Read exactly frame_len bytes
payload = tcp_socket.recv(frame_len)

# 3. Read variant index
variant_idx, offset = read_varint(payload, 0)

if variant_idx == 0:  # Push Response
    msg_id, _ = read_string(payload, offset)
    return msg_id
elif variant_idx == 7:  # Redirect
    leader_addr, _ = read_string(payload, offset)
    redirect_and_retry(leader_addr)
elif variant_idx == 8:  # Error
    err_msg, _ = read_string(payload, offset)
    raise ServerError(err_msg)
```
