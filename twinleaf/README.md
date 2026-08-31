# Twinleaf I/O Library in Rust

Library support for writing applications that work with Twinleaf quantum sensors and the Twinleaf I/O protocol.

The API is six nouns: a `Connection` owns the link and its I/O thread, `Connection::tree()` gives the `DeviceTree` it reaches, `DeviceTree::device(route)` mints a `Device`, and both hand out owned `Receiver`s of route-tagged `SampleBatch`es and `TreeEvent`s.

```rust
use std::time::Duration;
use twinleaf::{Connection, DeviceRoute};

let conn = Connection::open("tcp://localhost");
let device = conn.tree().device(DeviceRoute::root());

let name: String = device.get("dev.name")?;
println!("connected to {name}");

let batches = device.subscribe()?;
let batch = batches.recv_timeout(Duration::from_secs(5))?;
for row in batch.iter() {
    let values: Vec<_> = row.values().collect();
    println!("{} {values:?}", row.timestamp_end());
}
```

`twinleaf::data` holds the sample, metadata, and log vocabulary the batches speak, `twinleaf::firmware` updates instruments, and `twinleaf::tio` is the raw packet layer beneath all of it. For more, study how the API is used in `twinleaf-tools`.
