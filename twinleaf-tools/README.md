# Twinleaf I/O Tools in Rust

This repository contains a set of tools that are useful for working with Twinleaf quantum sensors and accessories. The repository contains the twinleaf library and a set of command line tools: a proxy, a utility, and a data monitoring tool.


## Proxy

The proxy makes the device's serial port available via ethernet, and supports multiple simultaneous client connections to a single device:

		tio-proxy --auto

When there are more than one serial port available, it is necessary to specify the port

		[linux]  > tio-proxy -r /dev/ttyACM0
		[macOS]  > tio-proxy -r /dev/cu.usbserialXXXXXX
		[windows]> tio-proxy.exe -r COM3

When a sensor is attached to a hub at port `0`, it is possible to proxy the data directly to that port using the `-s` flag:

		tio-proxy --auto -s /0

With the proxy running, a set of tools can be used on the data stream. 

Log data to a raw binary file:

		tio-tool log

Parse that log data for stream id 1 to a csv file:

		tio-tool log-csv 1 logfile.tio

Issue commands:

		tio-tool rpc dev.name

List available commands:

		tio-tool rpc-list

Firmware upgrade:

		tio-tool firmware-upgrade <firmware.bin>

Monitoring the data stream:

		tio-tool data-dump

A TUI data monitor:

		tio-monitor

And a variety of additional functions for use with Twinleaf sensors.


## Installation

The tools can be installed using

	cargo install twinleaf-tools

It is convenient to add the cargo binary directory to the default search paths. Cargo will report where the binaries and installed and which path to add to your environment, if necessary.

The `serialport` library depends on `libudev` that is not included on certain linux distributions. To install it use:

		sudo apt install libudev-dev # debian linux

## Cross compilation 

The tools can be compiled for other platforms by first adding those platform targets:

		rustup target add x86_64-pc-windows-gnu
		rustup toolchain install stable-x86_64-pc-windows-gnu

And then building for the new target:

		cargo build --target x86_64-pc-windows-gnu

