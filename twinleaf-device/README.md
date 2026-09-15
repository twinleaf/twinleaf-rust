What a Twinleaf device does with the packets it receives and the samples it
takes: the standard RPC table and its introspection, stream segments, metadata,
settings, log messages, and heartbeats.
[`twinleaf-proto`](https://docs.rs/twinleaf-proto) encodes the packets; this
crate is the state between them.

Nothing here waits. Every piece is a state machine stepped by whoever owns
the clock and the transport: a firmware task, a host tool, or a test. The
crate is `no_std`, allocation free, and depends only on `twinleaf-proto`, so
there is no executor to be independent of.
