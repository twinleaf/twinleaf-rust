The `twinleaf-data` crate holds the samples a host decodes from Twinleaf I/O
(`tio`) packets: their batches, the history kept of each stream, statistics
over its columns, and recorded log files with their exports. It reads packets
through [`twinleaf-proto`](https://docs.rs/twinleaf-proto) and knows nothing of
transports, tasks, sessions or RPC. The
[`twinleaf`](https://docs.rs/twinleaf) crate re-exports it as `twinleaf::data`.

The crate builds for `wasm32-unknown-unknown`. There a log is read with
`LogFile::from_bytes`, since `LogFile::open` memory-maps a file and exists
wherever there is an operating system.

## Cargo features

- `hdf5`: HDF5 export of recorded logs.
- `processing`: decimation and spectral estimates over a column.
- `test-util`: `fixtures`, for tests of code that uses this crate.
