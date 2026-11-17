"""Writes butterworth.txt: impulse responses of Butterworth filters as scipy
designs them, for tests/design.rs to compare the crate's own designs against.

Run it with a Python that has scipy:

    python3 tests/fixtures/butterworth.py > tests/fixtures/butterworth.txt
"""

import numpy as np
import scipy
from scipy import signal

SAMPLES = 96

CASES = [
    # kind, orders, low edge or cutoff, high edge, sample rate
    ("lowpass", [1, 2, 3, 4, 5, 8], 40.0, 0.0, 500.0),
    ("lowpass", [1, 2, 4, 6], 1.0, 0.0, 1000.0),
    ("highpass", [1, 2, 3, 4, 5, 8], 40.0, 0.0, 500.0),
    ("highpass", [1, 2, 4, 6], 1.0, 0.0, 1000.0),
    ("bandpass", [1, 2, 3, 4, 6], 1.0, 40.0, 500.0),
    ("bandpass", [1, 2, 3, 4], 45.0, 55.0, 1000.0),
    ("bandpass", [1, 2, 4], 0.1, 10.0, 500.0),
]

print(f"# scipy {scipy.__version__}: butter(order, edges, kind, fs=rate, output='sos'),")
print(f"# then sosfilt over a unit impulse, {SAMPLES} samples.")
print("# kind order low high rate : samples")
impulse = np.zeros(SAMPLES)
impulse[0] = 1.0
for kind, orders, low, high, rate in CASES:
    for order in orders:
        edges = [low, high] if kind == "bandpass" else low
        sos = signal.butter(order, edges, kind, fs=rate, output="sos")
        response = signal.sosfilt(sos, impulse)
        samples = " ".join(repr(float(x)) for x in response)
        print(f"{kind} {order} {low!r} {high!r} {rate!r} : {samples}")
