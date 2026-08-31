# Twinleaf I/O Library in Rust

Library support for writing applications that work with Twinleaf quantum sensors and the Twinleaf I/O protocol.

The high-level API follows the routed connection: a `Connection` owns the link
and its I/O thread, `Connection::tree(route)` gives a `DeviceTree` view rooted at
that route, and `Connection::device(route)` or `DeviceTree::device(route)` mints
a `Device`. All three views hand out owned `Receiver`s. Sample batches and
device events retain their absolute routes even when received from a
single-device view.

```rust
use std::time::Duration;
use twinleaf::{Connection, DeviceRoute};

let conn = Connection::open("tcp://localhost");
let device = conn.device(DeviceRoute::root());

let name: String = device.get("dev.name").expect("read device name");
println!("connected to {name}");

let batches = device.samples();
let batch = batches
    .recv_timeout(Duration::from_secs(5))
    .expect("receive a sample batch");
for row in batch.iter() {
    let values: Vec<_> = row.values().collect();
    println!("{} {values:?}", row.timestamp_end());
}
```

Use a `DeviceTree` when the route changes from one operation to the next:

```rust
use twinleaf::{Connection, DeviceRoute};

let conn = Connection::open("tcp://localhost");
let root = DeviceRoute::root();
let tree = conn.tree(root);
let name: String = tree.get(root, "dev.name").expect("read device name");
```

When several operations target the same route, `tree.device(route)` cheaply
binds it once and removes the route argument. Creating that `Device` validates
that the route is inside the tree view; the first operation still determines
whether a device actually exists there.

Most applications only need `Connection`, `Device` or `DeviceTree`, and
`SampleBatch`. Batch metadata is exposed as borrowed `twinleaf-proto` device,
stream, segment, and column fields. Call `SampleBatch::metadata()` only when an
application needs a small owned snapshot that outlives the batch. Protocol
identities such as `StreamId`, `SegmentId`, `ColumnId`, `SampleNumber`, and
`SessionId` are distinct newtypes; use `value()` only when crossing into
ordinary numeric code.

`twinleaf::data` also contains optional host-side facilities: `Buffer` and
`ColumnProcessor` support live applications such as an oscilloscope, while
`LogFile` and `PacketParser` support offline and lower-level tools.
`twinleaf::firmware` updates instruments, and `twinleaf::tio` is the raw packet
layer beneath all of it. For more, study how the API is used in
`twinleaf-tools`.
