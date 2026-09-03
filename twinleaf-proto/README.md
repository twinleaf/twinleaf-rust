`no_std` types for the Twinleaf I/O (TIO) wire protocol, as implemented by
[libtio](https://github.com/twinleaf/libtio). Hosts should use the
[`twinleaf`](https://docs.rs/twinleaf) crate, which builds on this one.

## Packet Format

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Type      |  TTL  |   R   |       Payload Length P        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload (P bytes)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Routing (R bytes)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Field          | Size   | Value                  |
|----------------|--------|------------------------|
| Type           | u8     | Packet type, see below |
| TTL            | 4 bits | Hops remaining, 0 to 15 |
| R              | 4 bits | Routing length, 0 to 8 |
| Payload Length | u16    | 0 to 500               |

Multi-byte values are little endian. [`packet`] parses and writes the header.

Routing holds the path in reverse order, one byte per hop: `/1/2/3` is stored
as `[3, 2, 1]`. A device receiving a packet with R = 0 handles it. Otherwise
it removes the last routing byte and forwards the packet to that port.
[`route`] handles routes and hops.

## Packet Types

| Value   | Type         | Payload                                  |
|---------|--------------|------------------------------------------|
| 1       | `LOG`        | Log message, [`log`]                     |
| 2       | `RPC_REQ`    | RPC request, [`rpc`]                     |
| 3       | `RPC_REP`    | RPC reply, [`rpc`]                       |
| 4       | `RPC_ERROR`  | RPC error, [`rpc`]                       |
| 5       | `HEARTBEAT`  | Session id or empty, [`heartbeat`]       |
| 6 to 8  | `LEGACY_*`   | Legacy stream descriptors, unsupported   |
| 11      | `METADATA`   | Stream metadata record, [`data`]         |
| 12      | `SETTING`    | Setting change, [`settings`]             |
| 62      | `SYNC`       | Time reference, [`sync`]                 |
| 63      | `TEXT`       | Console text                             |
| 64      | `USER`       | Application defined                      |
| 65      | `RPC_UPDATE` | Method value changed, [`rpc`]            |
| 128 + n | `STREAM` n   | Samples of stream n, [`data`]            |

Values 0, 9, 10, and 13 are invalid.

## RPC Formats

Some RPCs have payload formats defined here. [`identifiers`] has the typed
ids they use.

| RPC                                                   | Format                                                              |
|-------------------------------------------------------|---------------------------------------------------------------------|
| `rpc.info`, `rpc.listinfo`                            | [`rpc::RpcMeta`]                                                    |
| `dev.metadata`                                        | [`data::MetadataSelector`] request, [`data::MetadataReply`] reply   |
| Capture metadata                                      | [`capture::CaptureMetadata`]                                        |
| `dev.session`, `dev.revision`, `dev.serial`, `dev.firmware.serial` | [`SessionId`], [`HwRev`], [`DeviceSerial`], [`FirmwareSerial`] |

## Serial Framing

On a serial line each packet is followed by its CRC32 (ISO HDLC, little
endian) and SLIP encoded per RFC 1055. [`serial`] encodes and decodes frames.

| Code           | Value |
|----------------|-------|
| `SLIP_END`     | 0xC0  |
| `SLIP_ESC`     | 0xDB  |
| `SLIP_ESC_END` | 0xDC  |
| `SLIP_ESC_ESC` | 0xDD  |

TCP carries packets back to back on port 7855. UDP carries one packet per
datagram on port 7855. WebSocket carries one packet per binary message on
port 7853.

## Features

- `defmt`: derive `defmt::Format` on the wire types.
