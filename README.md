# Twinleaf Rust

Library and tools to interface with Twinleaf magnetometers.

https://twinleaf.com/

## Installation

On macOS and linux, there is a dependency on libudev; to install it use:

		sudo apt install libudev-dev  # debian linux
		brew install libudev          # macOS homebrew

Now build:

		cargo build --release

The tool can be run as follows:

		cd target/release
		./tio-tool

