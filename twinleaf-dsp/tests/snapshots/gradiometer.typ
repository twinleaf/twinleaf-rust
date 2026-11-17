// gradiometer.tsl, a signal graph drawn by twinleaf-dsp.
#import "@preview/autograph:0.1.0": diagram, node, edge
#import "@preview/fletcher:0.5.8": shapes

#let wire = (stroke: 0.6pt + black, mark: (end: ">", fill: black, scale: 0.8))

#diagram(
  bezier: true,
  node-shape: shapes.rect,
  node-stroke: 0.6pt + black,
  node-inset: 6pt,
  node-corner-radius: 2pt,

  node(<n0>, align(center)[#raw("/0/field") \ #text(0.8em, "1 kHz · 3 ch · nT")], shape: shapes.pill),
  node(<n1>, align(center)[#raw("/1/field") \ #text(0.8em, "500 Hz · 3 ch · nT")], shape: shapes.pill),
  node(<n2>, align(center)[#raw("decimate(2)") \ #text(0.8em, "500 Hz · 3 ch · nT")]),
  node(<n3>, align(center)[#raw("align") \ #text(0.8em, "500 Hz · 6 ch · nT")]),
  node(<n4>, align(center)[#raw("notch(line, q = 30)")]),
  node(<n5>, align(center)[#raw("bandpass(lo, hi, order = 4)")]),
  node(<n6>, align(center)[#raw(".a") \ #text(0.8em, "500 Hz · 3 ch · nT")]),
  node(<n7>, align(center)[#raw(".b") \ #text(0.8em, "500 Hz · 3 ch · nT")]),
  node(<n8>, align(center)[#raw("−")]),
  node(<out0>, align(center)[#strong(raw("/255/gradient")) \ #text(0.8em, "500 Hz · 3 ch · nT")], shape: shapes.pill, stroke: 1pt + black),

  edge(<n0>, <n2>, ..wire),
  edge(<n2>, <n3>, ..wire),
  edge(<n1>, <n3>, ..wire),
  edge(<n3>, <n4>, ..wire),
  edge(<n4>, <n5>, ..wire),
  edge(<n5>, <n6>, ..wire),
  edge(<n5>, <n7>, ..wire),
  edge(<n6>, <n8>, ..wire),
  edge(<n7>, <n8>, ..wire),
  edge(<n8>, <out0>, ..wire),
)
