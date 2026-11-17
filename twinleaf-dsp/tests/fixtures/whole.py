"""Writes whole.txt: what the functions that work on a whole run, filters
with `phase = zero` and `detrend`, should make of the recording that
tests/whole.rs synthesizes, worked out here with numpy and scipy and none of
the crate's code.

Run it with a Python that has scipy:

    python3 tests/fixtures/whole.py > tests/fixtures/whole.txt

The input is defined twice, here and in tests/common/mod.rs, by the same
formulas.
"""

import math

import numpy as np
import scipy
from scipy import signal

RATE = 1000.0
SAMPLES = 1200
COMMON = [1.0, 0.5, -0.25]
LOCAL = [3.0, 0.0, 1.5]


def noise(seed, channel, n):
    return ((n * 2654435761 + channel * 40503 + seed * 7919) % 1000003) / 1000003 - 0.5


def common(t):
    return 50.0 * math.sin(2 * math.pi * 7 * t) + 20.0 * math.sin(2 * math.pi * 60 * t)


def first_sensor(channel, n):
    t = n / RATE
    local = LOCAL[channel] * math.sin(2 * math.pi * 11 * t)
    return COMMON[channel] * common(t) + local + 0.01 * noise(1, channel, n)


X = np.array([[first_sensor(c, n) for n in range(SAMPLES)] for c in range(3)])


def notch(frequency, q):
    """The RBJ cookbook notch, as one second-order section."""
    w = 2 * math.pi * frequency / RATE
    alpha = math.sin(w) / (2 * q)
    b = np.array([1.0, -2 * math.cos(w), 1.0]) / (1 + alpha)
    a = np.array([1.0, -2 * math.cos(w) / (1 + alpha), (1 - alpha) / (1 + alpha)])
    return np.array([np.concatenate([b, a])])


def polynomial(x, degree):
    n = np.arange(len(x))
    return x - np.polyval(np.polyfit(n, x, degree), n)


def write(name, per_channel):
    for channel, values in zip("xyz", per_channel):
        print(name, channel, " ".join(repr(float(v)) for v in values))


low = signal.butter(4, 40.0, "lowpass", fs=RATE, output="sos")
band = signal.butter(3, [5.0, 80.0], "bandpass", fs=RATE, output="sos")

print(f"# scipy {scipy.__version__}, numpy {np.__version__}")
print("# name, channel, then every sample of the run")
write("low", [signal.sosfiltfilt(low, x) for x in X])
write("band", [signal.sosfiltfilt(band, x) for x in X])
write("notched", [signal.sosfiltfilt(notch(60.0, 30.0), x) for x in X])
write("level", [signal.detrend(x, type="constant") for x in X])
write("line", [signal.detrend(x, type="linear") for x in X])
write("curve", [polynomial(x, 2) for x in X])
write("both", [signal.sosfiltfilt(low, signal.detrend(x, type="linear")) for x in X])
