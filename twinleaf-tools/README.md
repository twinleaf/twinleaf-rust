# Twinleaf I/O Tools in Rust

Command-line tools for working with Twinleaf quantum sensors and accessories. Contains a proxy, terminal UIs, and command line utilities.

**Note**: In versions <2.0.0, this crate contained binaries named `tio-proxy`, `tio-monitor`, `tio-health`, and `tio-tool`. These commands have been packaged into subcommands under the single binary `tio`. The former three original commands can be simply used without the `-`, while `tio-tool {toolname}` calls have largely been replaced with `tio {toolname}`.

## CLI Usage
All the tools mentioned can have the `--help` argument added to display more information. The general workflow is to connect using `tio proxy` which allows all the CLI tools to work with the single device at the same time.

### Connecting to the device

The proxy makes a device attached via serial port available via Ethernet. The following will automatically scan for a `twinleaf` serial device:

		tio proxy --auto

When there are more than one serial port available, it is necessary to specify the port using:

		[linux] tio proxy -r /dev/ttyACM0
		[macOS] tio proxy -r /dev/cu.usbserialXXXXXX
		[wsl1]  tio proxy -r COM3

The proxy allows multiple tools to connect to the device over TCP simultaneously.

When a sensor is attached to a hub at port `0`, it is possible to proxy the data directly to that port using the `-s` flag:

		tio proxy --auto -s /0

### Interacting with the device in terminal

Logging metadata:

		tio log meta 			# Write metadata to meta.tio
		tio log dump meta.tio 	# Parse meta.tio and print to terminal

Logging sample data:

		# Log samples and write to log.{date-time}.tio until quit (Ctrl+C)
		tio log

		# Parse a stream from .tio file and write to csv
		tio log csv {stream name/id} {file}

		# Write to HDF5 file (requires `--features hdf5` on install)
		tio log hdf {file} -g "{route}/{stream}/{column}"
		tio log hdf {file} -g "*/{stream or column}"

Dump data to terminal:

		tio dump --data			# Continuously print parsed samples
		tio dump --data --meta	# Same as above, with metadata header

Device commands:

		tio rpc list			# List of rpc commands with types
		tio rpc {command} [arg] # Issue command with optional argument

## Terminal UIs

### tio monitor

Displays a live stream of incoming data with in-terminal graphs and command suggestions. Graph a stream by selecting it with the arrow keys and pressing Enter, and enter command mode by typing `:` (colon). Use `tio monitor -s {port}` to specify which port to open or `tio monitor -a` to open all ports.

Run with:

		tio monitor

### tio health

Displays a live table of all incoming data with some statistics to verify the behavior of devices. It also outputs a history of events derived from the `Sample` protocol implemented in the `twinleaf` crate, derived within the `data/buffer.rs` file.

Run with:

		tio health

## Installation

The tools can be installed using

		cargo install twinleaf-tools

They can be installed with the ability to convert to HDF5 using

		cargo install twinleaf-tools --features hdf5

It is convenient to add the cargo binary directory to the default search paths. Cargo will report where the binaries and installed and which path to add to your environment, if necessary.

The `serialport` library depends on `libudev` that is not included on certain linux distributions. To install it use:

		sudo apt install libudev-dev # debian linux

## Cross compilation

The tools can be compiled for other platforms by first adding those platform targets:

		rustup target add x86_64-pc-windows-gnu
		rustup toolchain install stable-x86_64-pc-windows-gnu

And then building for the new target:

		cargo build --target x86_64-pc-windows-gnu
