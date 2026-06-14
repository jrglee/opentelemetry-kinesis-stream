#!/usr/bin/env python3
"""Parse the perf harness output and emit a Markdown scenario breakdown.

The harness uses Go benchmarks (testing.B) plus per-iteration timestamps to
report `min_ns`, `p50_ns`, `p90_ns`, `max_ns`, `samples` alongside the
standard `ns/op` (mean). The Markdown tables show p50/p90 by default and
fold mean/min/max into expandable detail blocks per scenario.
"""
import argparse
import re
import sys
from collections import defaultdict

BENCH_LINE = re.compile(
    r'^Benchmark(?P<kind>Encode|Decode)(?P<signal>Metrics|Traces)/'
    r'(?P<profile>[^/]+)/(?P<enc>[^/]+)/(?P<codec>[^/]+)/n=(?P<n>\d+)-\d+\s+'
    r'(?P<iters>\d+)\s+(?P<ns>[\d.]+) ns/op'
)
SKIP_LINE = re.compile(
    r'^\s*Benchmark(?P<kind>Encode|Decode)(?P<signal>Metrics|Traces)/'
    r'(?P<profile>[^/]+)/(?P<enc>[^/]+)/(?P<codec>[^/]+)/n=(?P<n>\d+)-\d+\s*$'
)

# Custom metrics emitted as `<value> <name>` columns by testing.ReportMetric.
def find_metric(line, name):
    m = re.search(rf'([\d.eE+-]+)\s+{name}\b', line)
    return float(m.group(1)) if m else None

def parse(paths):
    rows = []
    for path in paths:
        try:
            lines = open(path).readlines()
        except FileNotFoundError:
            continue
        for line in lines:
            m = BENCH_LINE.match(line)
            if m:
                d = m.groupdict()
                rows.append({
                    'kind': d['kind'], 'signal': d['signal'].lower(),
                    'profile': d['profile'], 'enc': d['enc'], 'codec': d['codec'],
                    'n': int(d['n']),
                    'ns': float(d['ns']),
                    'cb': find_metric(line, 'compressed_bytes'),
                    'cr': find_metric(line, 'compression_ratio'),
                    'cbpr': find_metric(line, 'compressed_bytes_per_record'),
                    'rbpr': find_metric(line, 'raw_bytes_per_record'),
                    'min_ns': find_metric(line, 'min_ns'),
                    'p50_ns': find_metric(line, 'p50_ns'),
                    'p90_ns': find_metric(line, 'p90_ns'),
                    'max_ns': find_metric(line, 'max_ns'),
                    'samples': find_metric(line, 'samples'),
                    'bo': find_metric(line, 'B/op'),
                    'ao': find_metric(line, 'allocs/op'),
                    'skipped': False,
                })
                continue
            ms = SKIP_LINE.match(line)
            if ms and ' ns/op' not in line:
                d = ms.groupdict()
                rows.append({
                    'kind': d['kind'], 'signal': d['signal'].lower(),
                    'profile': d['profile'], 'enc': d['enc'], 'codec': d['codec'],
                    'n': int(d['n']),
                    'ns': None, 'cb': None, 'cr': None,
                    'min_ns': None, 'p50_ns': None, 'p90_ns': None,
                    'max_ns': None, 'samples': None, 'bo': None, 'ao': None,
                    'skipped': True,
                })
    return rows

def fmt_ns(x):
    if x is None: return '—'
    x = float(x)
    if x < 1e3: return f'{x:.0f}ns'
    if x < 1e6: return f'{x/1e3:.1f}µs'
    if x < 1e9: return f'{x/1e6:.2f}ms'
    return f'{x/1e9:.2f}s'

def fmt_bytes(x):
    if x is None: return '—'
    x = float(x)
    if x < 1024: return f'{int(x)}B'
    if x < 1024*1024: return f'{x/1024:.1f}KiB'
    return f'{x/1024/1024:.2f}MiB'

def fmt_ratio(x):
    if x is None: return '—'
    return f'{x:.2f}×'

def fmt_bpr(x):
    """Bytes per record. Sub-1 B/rec is meaningful (e.g. Arrow at large n);
    show two decimals for those, else just integer-ish."""
    if x is None: return '—'
    if x < 10: return f'{x:.2f}B'
    if x < 1024: return f'{x:.0f}B'
    return f'{x/1024:.1f}KiB'

def codec_order(): return ['none', 'gzip', 'zstd', 'snappy', 'x-snappy-framed', 'zlib', 'deflate']
def enc_order(): return ['otlp_proto', 'otlp_json', 'otel_arrow']
def batch_order(): return [1, 10, 100, 1000, 10000, 100000, 1000000]

def cells_for(rows_by_key, codec, n, encs):
    return [rows_by_key.get((codec, n), {}).get(e) for e in encs]

def emit_summary_table(out, rows, kind, signal, profile, header_level=3):
    encs = enc_order()
    out.append(f"{'#'*header_level} {kind} — {signal} / `{profile}`")
    out.append("")
    out.append(f"Latency: **p50** / p90 per call · size: compressed bytes · ratio: raw÷compressed.")
    out.append("")
    by = defaultdict(dict)
    for r in rows:
        if r['kind'] != kind or r['signal'] != signal or r['profile'] != profile:
            continue
        by[(r['codec'], r['n'])][r['enc']] = r

    if kind == 'Encode':
        sub_cols = ['p50', 'p90', 'B/rec', 'total', 'ratio']
    else:
        sub_cols = ['p50', 'p90', 'B/rec', 'total']
    header = ['codec', 'batch']
    for e in encs:
        for c in sub_cols:
            header.append(f'{e} {c}')
    out.append('| ' + ' | '.join(header) + ' |')
    out.append('|' + '|'.join(['---'] * len(header)) + '|')

    for codec in codec_order():
        for n in batch_order():
            cells = cells_for(by, codec, n, encs)
            if all(c is None for c in cells):
                continue
            row = [f'`{codec}`', f'{n:,}']
            for r in cells:
                if r is None or r.get('skipped') or r.get('ns') is None:
                    row.extend(['—'] * len(sub_cols))
                    continue
                row.append(fmt_ns(r['p50_ns']))
                row.append(fmt_ns(r['p90_ns']))
                row.append(fmt_bpr(r.get('cbpr')))
                row.append(fmt_bytes(r['cb']))
                if kind == 'Encode':
                    row.append(fmt_ratio(r['cr']))
            out.append('| ' + ' | '.join(row) + ' |')
    out.append("")

def emit_detail_table(out, rows, kind, signal, profile, header_level=4):
    encs = enc_order()
    out.append(f"{'#'*header_level} Detail (mean · min · max · samples · allocs) — {kind} {signal} / `{profile}`")
    out.append("")
    out.append("<details><summary>Expand</summary>")
    out.append("")
    by = defaultdict(dict)
    for r in rows:
        if r['kind'] != kind or r['signal'] != signal or r['profile'] != profile:
            continue
        by[(r['codec'], r['n'])][r['enc']] = r

    for codec in codec_order():
        for n in batch_order():
            cells = cells_for(by, codec, n, encs)
            if all(c is None or c.get('skipped') for c in cells):
                # Show skipped row for visibility
                if any(c is not None and c.get('skipped') for c in cells):
                    out.append(f"- `{codec}` n={n:,}: " + ', '.join(
                        f'{encs[i]}=skipped' for i, c in enumerate(cells) if c and c.get('skipped')))
                continue
            out.append(f"- `{codec}` n={n:,}:")
            for i, r in enumerate(cells):
                if r is None or r.get('skipped'):
                    out.append(f"  - {encs[i]}: —")
                    continue
                samples = int(r['samples']) if r['samples'] else 0
                out.append(
                    f"  - {encs[i]}: mean {fmt_ns(r['ns'])} · min {fmt_ns(r['min_ns'])} · "
                    f"max {fmt_ns(r['max_ns'])} · {samples} samples · "
                    f"{fmt_bytes(r['bo'])} alloc · {int(r['ao']) if r['ao'] else 0} allocs"
                )
    out.append("")
    out.append("</details>")
    out.append("")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--enc-metrics')
    ap.add_argument('--dec-metrics')
    ap.add_argument('--enc-traces')
    ap.add_argument('--dec-traces')
    ap.add_argument('--out')
    args = ap.parse_args()

    paths = [p for p in [args.enc_metrics, args.dec_metrics,
                          args.enc_traces, args.dec_traces] if p]
    rows = parse(paths)

    out = []
    out.append("<!-- generated by perf/format_perf.py — do not edit by hand -->")
    out.append("")

    metrics_profiles = ['metrics-high-frequency', 'metrics-balanced', 'metrics-high-cardinality']

    out.append("## Encode (encode + compress)")
    out.append("")
    for p in metrics_profiles:
        emit_summary_table(out, rows, 'Encode', 'metrics', p)
        emit_detail_table(out, rows, 'Encode', 'metrics', p)
    emit_summary_table(out, rows, 'Encode', 'traces', 'traces-typical')
    emit_detail_table(out, rows, 'Encode', 'traces', 'traces-typical')

    out.append("## Decode (decompress + decode)")
    out.append("")
    for p in metrics_profiles:
        emit_summary_table(out, rows, 'Decode', 'metrics', p)
        emit_detail_table(out, rows, 'Decode', 'metrics', p)
    emit_summary_table(out, rows, 'Decode', 'traces', 'traces-typical')
    emit_detail_table(out, rows, 'Decode', 'traces', 'traces-typical')

    text = "\n".join(out)
    if args.out:
        with open(args.out, 'w') as f:
            f.write(text)
    else:
        sys.stdout.write(text)

if __name__ == '__main__':
    main()
