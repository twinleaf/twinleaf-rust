# Twinleaf I/O Tool in Rust

This tool connects to a Twinleaf sensor attached to a serial port. The primary tool is the proxy, which makes the sensor available via ethernet:

		tio-tool proxy

when there are more than one serial port available, it is necessary to specify the port

		[linux]> tio-tool proxy -r /dev/ttyACM0
		[macOS]> tio-tool proxy -r /dev/cu.usbserialXXXXXX
		[wsl1] > tio-tool proxy -r COM3

There is also a simple RPC function:

		tio-tool rpc "dev.name"

## Installation

On macOS and linux, there is a dependency on libudev; to install it use:

		sudo apt install libudev-dev  # debian linux
		brew install libudev          # macOS homebrew

Now build:

		cargo build --release

The tool can be run as follows:

		cd target/release
		./tio-tool

## Cross compilation 

The tools can be compiled for other platforms by first adding those platform targets:

		rustup target add x86_64-pc-windows-gnu
		rustup toolchain install stable-x86_64-pc-windows-gnu

And then building for the new target:

		cargo build --target x86_64-pc-windows-gnu
