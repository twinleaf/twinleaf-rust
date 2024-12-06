# Twinleaf I/O Tools in Rust

This repository contains a set of tools that are useful for working with Twinleaf quantum sensors and accessories. 

The primary tool is the proxy, which makes the device available via ethernet:

		tio-proxy --auto

When there are more than one serial port available, it is necessary to specify the port

		[linux]> tio-proxy -r /dev/ttyACM0
		[macOS]> tio-proxy -r /dev/cu.usbserialXXXXXX
		[wsl1] > tio-proxy -r COM3

When a sensor is attached to a hub at port `0`, it is possible to proxy the data directly to that port using the `-s` flag:

		tio-proxy --auto -s /0

With the proxy running, a set of tools can be used on the data stream. 

Logging data to a raw binary file:

		tio-tool log

Parsing that log data for stream id 1 to a csv file:

		tio-tool log-csv 1 logfile.tio

Issuing commands:

		tio-tool rpc dev.name

List available commands:

		tio-tool rpc-list

Firmware upgrade:

		tio-tool firmware-upgrade <firmware.bin>

Monitoring the data stream:

		tio-tool data-dump

And a variety of additional functions for use with Twinleaf sensors.


## Installation

On macOS and linux, there is a dependency on libudev; to install it use:

		sudo apt install libudev-dev  # debian linux
		brew install libudev          # macOS homebrew

Now build:

		cargo build --release

The resulting tools can be found in the target directory:

		cd target/release
		./tio-tool

## Cross compilation 

The tools can be compiled for other platforms by first adding those platform targets:

		rustup target add x86_64-pc-windows-gnu
		rustup toolchain install stable-x86_64-pc-windows-gnu

And then building for the new target:

		cargo build --target x86_64-pc-windows-gnu
