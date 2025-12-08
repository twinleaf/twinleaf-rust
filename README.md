# Twinleaf I/O Tools in Rust

This repository contains a library and a set of tools that are useful for working with Twinleaf quantum sensors and accessories. 

## CLI Usage
All the tools mentioned can have the `--help` argument added to display more information. The general workflow is to connect using `tio-proxy` which allows all the CLI tools to work with the single device at the same time.

### tio-proxy

The proxy makes a device attached via serial port available via Ethernet. The following will automatically scan for a `twinleaf` serial device:

		tio-proxy --auto

When there are more than one serial port available, it is necessary to specify the port using:

		[linux]> tio-proxy -r /dev/ttyACM0
		[macOS]> tio-proxy -r /dev/cu.usbserialXXXXXX
		[wsl1] > tio-proxy -r COM3

The proxy allows multiple tools to connect to the device over TCP simultaneously.

### tio-tool

Logging data:
		
		tio-tool log

Issuing commands:
		
		tio-tool rpc dev.name

### tio-monitor

Displays a live stream of incoming data with an optional color-coded threshold option.

Running the tool:

		tio-monitor [yaml_path]

Yaml format to specify desired ranges:
		field_name: {min: 0.0, max: 10000.0}

### tio-health

Displays a live table of all incoming data with some statistics to verify the behavior of devices. It also outputs a history of events derived from the `Sample` protocol implemented in the `twinleaf` crate, derived within the `data/buffer.rs` file.

Running the tool:

		tio-health

## Installation

With rust language tools, install the tools using:

		cargo install twinleaf-tools

It can be installed with the ability to convert to HDF5 using

		cargo install twinleaf-tools --features hdf5