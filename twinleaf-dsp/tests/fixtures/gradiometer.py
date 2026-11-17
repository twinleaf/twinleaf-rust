"""Writes gradiometer.txt: what examples/gradiometer.tsl should make of the
recording that tests/run.rs synthesizes, worked out here with numpy and scipy
and none of the crate's code.

Run it with a Python that has scipy:

    python3 tests/fixtures/gradiometer.py > tests/fixtures/gradiometer.txt

The input is defined twice, here and in tests/run.rs, by the same formulas.
"""

import math

import numpy as np
import scipy
from scipy import signal

FAST, SLOW = 1000.0, 500.0
FAST_SAMPLES, SLOW_SAMPLES = 2400, 1200
COMMON = [1.0, 0.5, -0.25]  # how much of the common field each axis sees
LOCAL = [3.0, 0.0, 1.5]  # a source near the first sensor only


def noise(seed, channel, n):
    return ((n * 2654435761 + channel * 40503 + seed * 7919) % 1000003) / 1000003 - 0.5


def common(t):
    return 50.0 * math.sin(2 * math.pi * 7 * t) + 20.0 * math.sin(2 * math.pi * 60 * t)


def first_sensor(channel, n):
    t = n / FAST
    local = LOCAL[channel] * math.sin(2 * math.pi * 11 * t)
    return COMMON[channel] * common(t) + local + 0.01 * noise(1, channel, n)


def second_sensor(channel, n):
    return COMMON[channel] * common(n / SLOW) + 0.01 * noise(2, channel, n)


def lowpass(edge):
    """A Kaiser-windowed sinc that is flat to 0.8 of `edge` cycles per sample
    and 90 dB down from `edge` on, centred, with unit gain at zero."""
    transition = 0.2 * edge
    half = math.ceil((90.0 - 7.95) / (2.285 * 2 * math.pi * transition) / 2)
    beta = 0.1102 * (90.0 - 8.7)
    cutoff = edge - transition / 2
    t = np.arange(-half, half + 1, dtype=float)
    window = np.i0(beta * np.sqrt(1 - (t / half) ** 2)) / np.i0(beta)
    taps = 2 * cutoff * np.sinc(2 * cutoff * t) * window
    return half, taps / taps.sum()


def notch(frequency, q, rate):
    """The RBJ cookbook notch, as (b, a)."""
    w0 = 2 * math.pi * frequency / rate
    alpha = math.sin(w0) / (2 * q)
    b = np.array([1.0, -2 * math.cos(w0), 1.0])
    a = np.array([1 + alpha, -2 * math.cos(w0), 1 - alpha])
    return b / a[0], a / a[0]


a = np.array([[first_sensor(c, n) for n in range(FAST_SAMPLES)] for c in range(3)])
b = np.array([[second_sensor(c, n) for n in range(SLOW_SAMPLES)] for c in range(3)])

# a | decimate(2): sample k is input sample 2k, read through the low-pass.
half, taps = lowpass(0.25)
first = math.ceil(half / 2)
end = math.ceil((FAST_SAMPLES - half) / 2)
halved = np.array(
    [[taps @ row[2 * k - half : 2 * k + half + 1] for k in range(first, end)] for row in a]
)

# align(a | decimate(2), b): both count from the same instant, so the result
# is where both have samples.
end = min(end, SLOW_SAMPLES)
arms = [halved[:, : end - first], b[:, first:end]]

# | notch(line, q = 30) | bandpass(lo, hi, order = 4), from rest.
nb, na = notch(60.0, 30.0, SLOW)
sos = signal.butter(4, [1.0, 40.0], "bandpass", fs=SLOW, output="sos")
clean = [signal.sosfilt(sos, signal.lfilter(nb, na, arm, axis=1), axis=1) for arm in arms]

# clean.a - clean.b
gradient = clean[0] - clean[1]

print(f"# scipy {scipy.__version__}, numpy {np.__version__}")
print("# first: the index of the first sample; then one line per channel")
print(f"first {first}")
for name, row in zip("xyz", gradient):
    print(name, " ".join(repr(float(v)) for v in row))
