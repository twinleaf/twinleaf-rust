# Twinleaf I/O Tools in Rust

Command-line tools for working with Twinleaf quantum sensors and accessories,
packaged as one binary, `tio`.

**Note**: In versions <2.0.0, this crate contained binaries named `tio-proxy`,
`tio-monitor`, `tio-health`, and `tio-tool`. These commands are now subcommands
of `tio`: `tio proxy`, `tio monitor`, `tio health`, and `tio {toolname}` in
place of `tio-tool {toolname}`.

## Commands

| Command           | Purpose                                            |
|-------------------|----------------------------------------------------|
| `tio list`        | Discover devices on serial ports and the network   |
| `tio proxy`       | Multiplex a sensor over TCP                        |
| `tio monitor`     | Live sensor data display                           |
| `tio health`      | Live timing and rate diagnostics                   |
| `tio dump`        | Dump raw packets from a device                     |
| `tio log`         | Log samples to a file, and convert or inspect logs |
| `tio rpc`         | Execute a device RPC                               |
| `tio capture`     | Trigger and read a capture RPC                     |
| `tio upgrade`     | Upgrade device firmware                            |
| `tio simulate`    | Run a simulated sine wave device over UDP          |
| `tio completions` | Generate shell completions for `tio`               |

Every command that talks to a device takes `-r {url}` for the root address,
default `auto`, and `-s {route}` for a sensor in the tree, default
`/`. Run `tio {command} --help` for the full options.

## Sharing devices

Tools share devices through a proxy on `localhost:7855`, so several tools can
use one sensor at once. The first tool starts the proxy in the background, and
the proxy exits ten seconds after the last tool closes. If a device resets or
is unplugged, the proxy reconnects to it and the tools keep running.

With no `-r`, a tool uses the device `tio proxy` serves, the device another
tool already has open, or the only Twinleaf serial device attached. If there
is more than one choice, it lists them, and `-r {url}` or `tio list` picks one.

To run the proxy yourself, in the foreground:

```sh
tio proxy
```

Ctrl-C stops it, and the tools using it exit. To keep it running after the
terminal closes, use `nohup tio proxy &` or a service manager such as systemd
or launchd. `tio proxy stop` stops a proxy that a tool started in the
background.

To use a proxy on another port, name it with `-r`:

```sh
tio proxy --port 17855 serial:///dev/ttyACM0
tio monitor -r tcp://localhost:17855
```

A loopback URL like this connects to that proxy and uses its default device.
This is also how tio 1.x tools connect, and how to reach a 1.x `tio proxy`.

A background proxy logs to `proxy.log` in `$XDG_RUNTIME_DIR/twinleaf`, or in
`twinleaf/runtime` under the local data directory.

## Connecting to a device

`tio proxy` serves one device as the default for every tool, and over TCP to
other machines too. With no arguments it looks for a single Twinleaf serial
device:

```sh
tio proxy
```

With more than one serial port, give the URL:

```sh
tio proxy serial:///dev/ttyACM0            # linux
tio proxy serial:///dev/cu.usbserialXXXXXX # macOS
tio proxy serial://COM3                    # wsl1
```

`tio list` finds devices on serial ports and, over mDNS, on the network.
Selecting devices hosts them as the default for every tool until you press
Ctrl-C:

```sh
tio list
tio list --local   # serial ports only
```

Use ↑/↓ to browse the tree and ←/→ to collapse or expand nodes.
Tab/Shift+Tab switch devices. Enter uses the selected subtree; Space
marks multiple subtrees to serve together.

Serve several devices from one proxy by mounting each at a route prefix, or
narrow a hub to one subtree:

```sh
tio proxy --mount serial:///dev/ttyACM0=/0 --mount tcp://192.168.1.20=/1
tio proxy -s /0
```

`tio proxy --mdns` advertises the proxy on the network so `tio list` on
another machine finds it. `tio proxy nmea` serves the sensor's data as an NMEA
stream on TCP port 7800.

## Logging

```sh
tio log                      # samples to log.{date-time}.tio until Ctrl+C
tio log -f run.tio --duration 5m
tio log meta                 # metadata to meta.tio
```

Reading logs back:

```sh
tio log inspect {file}                 # summarize what a log holds
tio log dump {file} --data --meta      # print samples and metadata
tio log csv {stream name/id} {file}    # one stream to CSV
tio log hdf {file} -g "{route}/{stream}/{column}"   # HDF5, needs --features hdf5
tio log meta reroute {file} -s /0/1    # rewrite the route in a metadata file
```

## Live data

```sh
tio dump --data          # print parsed samples
tio dump --data --meta   # with metadata at boundaries
tio dump --data -g "/0/vector"
```

## Device commands

```sh
tio rpc list                    # RPCs with their types
tio rpc {name} [arg]            # call one, with an optional argument
tio rpc {name} {arg} -t f32     # give the argument type when it is ambiguous
tio rpc dump {name}             # read a large RPC value
tio capture {name}              # trigger a capture RPC and read its data
```

## Firmware

```sh
tio upgrade                 # latest published firmware for the connected sensor
tio upgrade {file}          # a specific image
tio upgrade --downgrade     # pick from every published release
tio upgrade --all -y        # every device on the hub that has a newer release
```

## Terminal UIs

### tio monitor

A live stream of incoming data with in-terminal graphs and command
suggestions. Select a stream with the arrow keys and press Enter to graph it.
Type `:` to enter command mode. `--depth N` limits how deep the device tree is
traversed.

```sh
tio monitor
tio monitor -s /0/1
```

### tio health

A live table of every incoming stream with sample rate, jitter, and drift
statistics, and a history of events such as segment changes, gaps, and
heartbeats.

```sh
tio health
tio health --warnings-only
```

## Simulation

`tio simulate` runs a fake device over UDP that streams a sine wave, for
testing the tools without hardware:

```sh
tio simulate
tio proxy udp://localhost
```

## Shell completions

`tio` can generate completion scripts for bash, zsh, fish, and PowerShell. Add
the matching line to your shell's config file:

```sh
# Bash (~/.bashrc)
eval "$(tio completions bash)"

# Zsh (~/.zshrc)
eval "$(tio completions zsh)"

# Fish (~/.config/fish/config.fish)
tio completions fish | source

# PowerShell ($PROFILE)
tio completions powershell | Invoke-Expression
```

Run `tio completions --help` for the full list of supported shells. Zsh users
may need to prepend `autoload -Uz compinit && compinit`.

## Installation

```sh
cargo install twinleaf-tools
cargo install twinleaf-tools --features hdf5   # with HDF5 conversion
```

Cargo reports where the binary is installed and which path to add to your
environment, if necessary.

The `serialport` library depends on `libudev`, which some Linux distributions
do not include:

```sh
sudo apt install libudev-dev   # debian linux
```

## Cross compilation

Add the target platform:

```sh
rustup target add x86_64-pc-windows-gnu
rustup toolchain install stable-x86_64-pc-windows-gnu
```

Then build for it:

```sh
cargo build --target x86_64-pc-windows-gnu
```
