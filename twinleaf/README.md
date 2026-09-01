The `twinleaf` crate provides a high-level API for communicating with Twinleaf
devices over the Twinleaf I/O (`tio`) protocol. It builds on
[`twinleaf-proto`](https://docs.rs/twinleaf-proto), which provides the lower-level
wire types and codecs.

## Capabilities

- Connect over serial, TCP, or UDP through [`Connection::open`].
- Discover serial devices and, with the [`mdns`](#cargo-features) Cargo feature,
  networked devices through [`Discovery`].
- Address a routed subtree with [`DeviceTree`] or bind one route with [`Device`].
- Call typed or dynamic RPCs and subscribe to [`SampleBatch`]es, [`Event`]s, or
  raw [`Packet`]s.
- Buffer and process live data or read recorded `tio` logs with the [data module].
- Query and update device firmware through the [firmware module].

## API model

All views share one connection and its background workers:

```text
Connection              scope: all reachable routes
├── DeviceTree at /1    scope: /1 and its descendants; RPCs take a route
└── Device at /1/2      scope: /1/2 only; route is already bound
```

Subscriptions return owned [`Receiver`]s rather than async streams. The
connection drives I/O on background threads, so receiving does not require an
async runtime or a caller-managed polling loop. Each receiver is filtered to
the scope of the view that created it.

## Example

```rust,no_run
use std::time::Duration;
use twinleaf::{Connection, DeviceRoute};

fn main() {
    let connection = Connection::open("tcp://localhost");
    let device = connection.device(DeviceRoute::root());

    let name: String = device.get("dev.name").expect("read device name");

    let samples = device.samples();
    let batch = samples
        .recv_timeout(Duration::from_secs(5))
        .expect("receive sample data");

    println!("{name}: {} rows from {}", batch.len(), batch.stream().name);
}
```

## Cargo features

- `serial` (default): serial connections and serial-device discovery.
- `firmware-update` (default): the [GitHub-backed firmware catalog]. Firmware
  queries and flashing are always available without this feature.
- `mdns`: discovery of networked devices through mDNS/DNS-SD.
- `hdf5`: HDF5 data export.

[`Connection::open`]: https://docs.rs/twinleaf/latest/twinleaf/device/struct.Connection.html#method.open
[`Device`]: https://docs.rs/twinleaf/latest/twinleaf/device/struct.Device.html
[`DeviceTree`]: https://docs.rs/twinleaf/latest/twinleaf/device/struct.DeviceTree.html
[`Discovery`]: https://docs.rs/twinleaf/latest/twinleaf/device/discovery/struct.Discovery.html
[`Event`]: https://docs.rs/twinleaf/latest/twinleaf/device/enum.Event.html
[`Packet`]: https://docs.rs/twinleaf/latest/twinleaf/tio/proto/struct.Packet.html
[`Receiver`]: https://docs.rs/twinleaf/latest/twinleaf/device/struct.Receiver.html
[`SampleBatch`]: https://docs.rs/twinleaf/latest/twinleaf/data/struct.SampleBatch.html
[data module]: https://docs.rs/twinleaf/latest/twinleaf/data/index.html
[firmware module]: https://docs.rs/twinleaf/latest/twinleaf/firmware/index.html
[GitHub-backed firmware catalog]: https://github.com/twinleaf/twinleaf-firmware-updates
