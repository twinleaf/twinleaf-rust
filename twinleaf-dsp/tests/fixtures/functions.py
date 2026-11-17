"""Writes functions.txt: what integrate, derivative, rms and decorrelate
should make of the signals tests/functions.rs synthesizes, worked out here
with numpy and scipy from each function's definition and none of the crate's
code.

Run it with a Python that has scipy:

    python3 tests/fixtures/functions.py > tests/fixtures/functions.txt

The input is defined twice, here and in tests/functions.rs, by the same
formulas.
"""

import math
import sys

import numpy as np
import scipy
from scipy import integrate

RATE = 200.0
SAMPLES = 600
TINY = sys.float_info.min


def noise(seed, channel, n):
    return ((n * 2654435761 + channel * 40503 + seed * 7919) % 1000003) / 1000003 - 0.5


def common(channel, n):
    t = n / RATE
    wave = math.sin(2 * math.pi * 3 * t) if channel == 0 else math.cos(2 * math.pi * 5 * t)
    return wave + 0.5 * noise(3, channel, n)


def reference(channel, n):
    return common(channel, n) + 0.02 * noise(4, channel, n)


def signal(channel, n):
    t = n / RATE
    if channel == 0:
        local = 0.3 * math.sin(2 * math.pi * 11 * t)
        return 2.0 * common(0, n) - 0.7 * common(1, n) + local + 0.01 * noise(5, 0, n)
    return (1.0 + 0.5 * t) * common(0, n) + 0.1 * noise(5, 1, n)


A = np.array([[signal(c, n) for n in range(SAMPLES)] for c in range(2)])
R = np.array([[reference(c, n) for n in range(SAMPLES)] for c in range(2)])


def window(seconds):
    return max(1, round(seconds * RATE))


def regressors(taps):
    """Row n holds each reference channel's last `taps` samples, most recent
    first, with zeros before the run began."""
    rows = np.zeros((SAMPLES, R.shape[0] * taps))
    for n in range(SAMPLES):
        for k in range(R.shape[0]):
            for lag in range(taps):
                if n - lag >= 0:
                    rows[n, k * taps + lag] = R[k, n - lag]
    return rows


def least_squares(x, taps, seconds):
    rows, length = regressors(taps), window(seconds)
    size = rows.shape[1]
    out = np.empty(SAMPLES)
    for n in range(SAMPLES):
        held = slice(max(0, n - length + 1), n + 1)
        u, held_x = rows[held], x[held]
        moments = u.T @ u
        stiffened = moments + (1e-12 * np.trace(moments) / size + TINY) * np.eye(size)
        weights = np.linalg.solve(stiffened, u.T @ held_x)
        out[n] = x[n] - rows[n] @ weights
    return out


def nlms(x, taps, seconds):
    rows, length = regressors(taps), window(seconds)
    size = rows.shape[1]
    step = min(1.0, size / length)
    weights, power = np.zeros(size), 0.0
    out = np.empty(SAMPLES)
    for n in range(SAMPLES):
        u = rows[n]
        energy = u @ u
        power += (energy - power) / min(n + 1, length)
        error = x[n] - weights @ u
        weights = weights + step * error * u / (max(energy, power) + TINY)
        out[n] = error
    return out


def rls(x, taps, seconds):
    rows, length = regressors(taps), window(seconds)
    size = rows.shape[1]
    forgetting = 1.0 - 1.0 / max(length, 2)
    weights, inverse = np.zeros(size), None
    out = np.empty(SAMPLES)
    for n in range(SAMPLES):
        u = rows[n]
        if inverse is None:
            energy = u @ u
            if not energy > 0:
                out[n] = x[n]
                continue
            inverse = np.eye(size) * (size / (0.01 * energy))
        projected = inverse @ u
        gain = projected / (forgetting + u @ projected)
        error = x[n] - weights @ u
        weights = weights + gain * error
        inverse = (inverse - np.outer(gain, projected)) / forgetting
        inverse = 0.5 * (inverse + inverse.T)
        out[n] = error
    return out


def rms(x, seconds):
    length = window(seconds)
    return np.array(
        [math.sqrt(np.mean(x[max(0, n - length + 1) : n + 1] ** 2)) for n in range(SAMPLES)]
    )


def write(name, first, per_channel):
    for channel, values in zip("xy", per_channel):
        print(name, channel, first, " ".join(repr(float(v)) for v in values))


print(f"# scipy {scipy.__version__}, numpy {np.__version__}")
print("# name, channel, the index of the first sample, then the samples")
write("integrate", 0, [integrate.cumulative_trapezoid(x, dx=1 / RATE, initial=0) for x in A])
write("derivative", 1, [np.gradient(x, 1 / RATE)[1:-1] for x in A])
write("rms", 0, [rms(x, 0.25) for x in A])
write("ls", 0, [least_squares(x, 1, 0.5) for x in A])
write("ls3", 0, [least_squares(x, 3, 0.5) for x in A])
write("nlms", 0, [nlms(x, 4, 0.5) for x in A])
write("rls", 0, [rls(x, 2, 0.5) for x in A])
