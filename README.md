# Twinleaf I/O Tools in Rust

This repository contains two Rust crates, a library (`twinleaf`) and a set of tools (`twinleaf-tools`) that are useful for working with Twinleaf quantum sensors and accessories.

**Note**: In versions <2.0.0, this crate contained binaries named `tio-proxy`, `tio-monitor`, `tio-health`, and `tio-tool`. These commands have been packaged into subcommands under the single binary `tio`. The former three original commands can be simply used without the `-`, while `tio-tool {toolname}` calls have largely been replaced with `tio {toolname}`.

## Basic Usage
### See `twinleaf-tools/README.md` for more usage details

Connect a proxy to the device to communicate with the other tools:

		tio proxy

Terminal interface (TUI) to monitor sample data and issue commands with tab completion:

		tio monitor

Dump data to terminal:

		tio dump --data --meta

Issue commands from terminal:

		tio rpc {command name} [arg]

Log data to csv:

		tio log -f {file.tio} # blocking
		tio log csv {stream name} {file.tio}

## Installation

With rust language tools, install the tools using:

		cargo install twinleaf-tools

It can be installed with the ability to convert to HDF5 using

		cargo install twinleaf-tools --features hdf5

It is convenient to add the cargo binary directory to the default search paths. Cargo will report where the binaries and installed and which path to add to your environment, if necessary.

The `serialport` library depends on `libudev` that is not included on certain linux distributions. To install it use:

		sudo apt install libudev-dev # debian linux
