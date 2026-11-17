# twinleaf-dsp

Signal graphs for Twinleaf sensors. A signal graph is a few lines of text that
say how sensor streams become derived ones, by filtering, resampling, aligning,
subtracting and decorrelating them. This crate is the language, the engine that
runs a graph, and the device that serves one.
[`twinleaf-tools`](https://crates.io/crates/twinleaf-tools) runs it as
`tio dsp`.

A graph is served as a TIO device beside the sensors it reads. Its outputs are
streams and its parameters are RPCs, so every tool that can plot, log or set a
sensor can do the same with a graph, and none of them needs to know there is
one.

## A graph

```text
# gradiometer.tsl: a software gradiometer from two vector magnetometers.

source a = /0/field  @rate(1000) @channels(x, y, z) @unit(nT)
source b = /1/field  @rate(500)  @channels(x, y, z) @unit(nT)

param line : Hz = 60        [45 .. 65]
param lo   : Hz = 1         [0.1 .. 10]
param hi   : Hz = 40        [10 .. 200]

# One time grid, six channels: a.x a.y a.z b.x b.y b.z.
let fr = align(a | decimate(2), b)

# One filter chain over all six, so the two arms cannot drift in phase.
let clean = fr | notch(line, q = 30) | bandpass(lo, hi, order = 4)

out gradient : nT = clean.a - clean.b
```

`source` names a stream of a sensor in the tree. `param` is a number that can
be set while the graph runs. `let` names a signal on the way, `fn` names a
pipeline to use more than once, and `out` names a stream to publish. A `|`
hands a signal to a function, arithmetic works channel by channel, and `.a`
picks out the channels that came from `a`.

The checker holds a graph to three things, and says so in the terms of the
text when it does not hold:

- **A rate never changes unasked.** Signals at different rates do not combine
  until one is decimated or resampled to match. `align` refuses to guess.
- **Units are checked.** Adding nT to V is an error, and `in` converts.
- **Channels pair by name**, never by position.

Each diagnostic has a code, a place in the text, and where it can a
suggestion.

## Functions

| Function | What it gives |
|---|---|
| `align(a, b, …)` | Signals of one rate on one time grid, as one signal |
| `decimate(n)` | The rate lowered by a whole factor, anti-aliased |
| `resample(rate)` | Any other rate |
| `notch(f, q = 30, harmonics = 1, phase = causal)` | A line and its harmonics removed |
| `bandpass(lo, hi, order = 4, phase = causal)` | Butterworth band-pass |
| `lowpass(f, order = 4, phase = causal)` | Butterworth low-pass |
| `highpass(f, order = 4, phase = causal)` | Butterworth high-pass |
| `detrend(mode)` | The mean, a line or a parabola fitted to the run and taken out |
| `magnitude(v)` | The length of a vector, as one channel |
| `decorrelate(x, ref = …, window = …, method = ls, taps = 1)` | What `ref` explains of `x` removed, by sliding least squares, NLMS or RLS |
| `psd(window)` | Power spectral density, by Welch's method |
| `rms(window)` | Root mean square over a sliding window |
| `integrate()` | The integral over time |
| `derivative()` | The slope over time |

`tio dsp functions` prints the list as the build at hand has it.

A filter with `phase = zero`, `detrend` and `psd` need a whole run before they
can say anything. They work over a recording and are refused by a live graph.

## The command line

```sh
tio dsp check gradiometer.tsl       # check a spec, and say what it will publish
tio dsp graph gradiometer.tsl --dot # draw it
tio dsp run gradiometer.tsl --log session.tio --csv gradient.csv
tio dsp serve gradiometer.tsl --mount tcp://sensor-a.local --mount serial:///dev/ttyACM0 --port 7900
```

`check` exits with 1 if the spec has an error and 2 if the command line or a
file is wrong. `run` goes over a recording. Its output is a function of the
samples alone, bit for bit the same for any `--block` and any `--threads`.

`serve` mounts the sensors at `/0`, `/1` and so on, in the order given, and
puts the graph beside them at `/255`:

```sh
tio monitor -r tcp://localhost:7900 -s /255
tio rpc -r tcp://localhost:7900 -s /255 dsp.line 50
```

What the device at `/255` has:

| | |
|---|---|
| Streams | One per `out`, named for it, with its channels as columns, their units and its rate, timed on the sensors' own clock |
| Settings | `dsp.<param>` for each `param`, with read-only `dsp.<param>.min` and `.max` where the spec gives a range |
| The spec | `dsp.spec.size`, `.read`, `.write` and `.apply`, which read the text that is running and replace it |
| Log | A note for each text taken and each param set, and a warning or error, with its place in the spec, for a text refused |

A text that does not check is refused and the one that was running carries on.
When a new text is taken, the stages the change did not reach keep their state.

## Figures

`graph --typst` prints the graph as a Typst figure, drawn with
[autograph](https://typst.app/universe/package/autograph), to include in a
paper or edit. `--pdf` and `--svg` compile it, with the `typst` on the `PATH`,
or in process when the crate is built with the `typst` feature.

## A build with functions of its own

The command line is in this library, so a build that adds functions to the
public ones is a `main` of a few lines. Its algorithms stay in its own binary.
What leaves that binary is a TIO device like any other.

```rust
use std::process::ExitCode;
use twinleaf_dsp::Library;

fn main() -> ExitCode {
    let library = Library::standard()
        .named("labdsp")
        .with(despike::FUNCTION, despike::build)
        .expect("`despike` is not a public function's name");
    twinleaf_dsp::cli::run(library)
}
```

A function is a `lang::builtins::Builtin`, which says how it is written and
how its result is typed, and a `Node`, which computes it. `labdsp check`,
`labdsp serve` and the rest then work as `tio dsp` does, with `despike` among
the functions.

## Features

| Feature | Default | What it adds |
|---|---|---|
| `serial` | yes | Sensors on serial ports, for `serve` |
| `cli` | yes | The command line, in `twinleaf_dsp::cli` |
| `serde` | no | A spec's outline as JSON, for an editor |
| `typst` | no | Figures compiled in process. It is a large dependency |

## Tests

The expected values in the tests are worked out independently, by the numpy
and scipy scripts in `tests/fixtures`, and are never taken from the engine's
own output. A rendered diagnostic or figure that changes on purpose is
accepted by running its test with `DSP_BLESS=1`, and reading the diff.
