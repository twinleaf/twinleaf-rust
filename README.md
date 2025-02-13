# Twinleaf I/O Tools in Rust

This repository contains a library and a set of tools that are useful for working with Twinleaf quantum sensors and accessories. 

## tio-proxy

The proxy makes a device attached via serial port available via ethernet. To use it:

		tio-proxy --auto

When there are more than one serial port available, it is necessary to specify the port using:

		[linux]> tio-proxy -r /dev/ttyACM0
		[macOS]> tio-proxy -r /dev/cu.usbserialXXXXXX
		[wsl1] > tio-proxy -r COM3

With the proxy running, a set of tools can be used on the data stream. 

## tio-tool

Logging data:
		
		tio-tool log

Issuing commands:
		
		tio-tool rpc dev.name

There are a variety of additional useful functions. To see all tool options run:

		tio-tool --help

## tio-monitor

Displays a live stream of incoming data with an optional color-coded threshold option.

Running the tool:

		tio-monitor [yaml_path]

Yaml format to specify desired ranges:
		field_name: {min: 0.0, max: 10000.0}

## Installation

With rust language tools, install the tools using:

		cargo install twinleaf-tools
