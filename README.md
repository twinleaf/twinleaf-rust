# Twinleaf I/O Tools in Rust

This repository contains three Rust crates: a library ([`twinleaf`](https://crates.io/crates/twinleaf)), a set of tools ([`twinleaf-tools`](https://crates.io/crates/twinleaf-tools)) that are useful for working with Twinleaf quantum sensors and accessories, and a Rust protocol implementation ([`twinleaf-proto`](https://crates.io/crates/twinleaf-proto)) of TIO.

It is highly recommended to install `twinleaf-tools` when using the `twinleaf` library.

## Installation

With rust language tools, install the tools using:

```sh
cargo install twinleaf-tools
```

It can be installed with the ability to convert to HDF5 using

```sh
cargo install twinleaf-tools --features hdf5
```

## Basic Usage

<details open>
<summary><code>tio list</code></summary>

Interactive picker for `tio proxy` (press space to mount multiple Twinleaf devices).

![tio list](docs/tio-list.gif)

</details>

<details>
<summary><code>tio monitor</code></summary>

Live sample data with in-terminal graphs. Press `:` to issue commands with tab completion.

![tio monitor](docs/tio-monitor.gif)

</details>

<details>
<summary><code>tio health</code></summary>

Live sample rate, jitter, and drift diagnostics with an event log. Press `:` to issue commands with tab completion.

![tio health](docs/tio-health.gif)

</details>

Run `tio --help` for the full command list, and see [`twinleaf-tools/README.md`](twinleaf-tools/README.md) for logging, conversion, and proxy details.

## Further Installation

It is convenient to add the cargo binary directory to the default search paths. Cargo will report where the binaries are installed and which path to add to your environment, if necessary.

The `serialport` library depends on `libudev` that is not included on certain linux distributions. To install it use:

```sh
sudo apt install libudev-dev # debian linux
```
