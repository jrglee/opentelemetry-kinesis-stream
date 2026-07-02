package awskinesisexporter

import (
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// signal_traces.go, signal_metrics.go, and signal_logs.go each hold one
// signal's codec constructor, groupers, splitHalf, and truncateAttributes.
// The split is forced by pdata's three disjoint type families (ptrace /
// pmetric / plog): every operation that touches signal data requires a
// separate concrete implementation. The signalCodec[T] seam in record.go
// is what keeps the group → compress → oversize-repack → PutRecords
// pipeline signal-agnostic; only the functions registered there touch pdata.

// tagSep separates joined tag values in a partition-key seed. 0x1f (unit
// separator) cannot appear in attribute keys and is vanishingly unlikely in
// values, so it avoids "a"+"bc" colliding with "ab"+"c".
const tagSep = "\x1f"

// clampStringAttrs truncates every string-valued attribute in m whose UTF-8
// byte length exceeds maxBytes, in place. Returns the number of values changed.
// Non-string kinds (bool, int, double, bytes, slice, map) are never touched —
// numeric values do not bloat records, and mutating structured kinds would
// rewrite semantics rather than trim them. The truncation backsteps to a
// codepoint boundary so the output remains valid UTF-8; otlp_json silently
// substitutes the replacement character on invalid sequences, which is worse
// than emitting a few fewer bytes than maxBytes.
func clampStringAttrs(m pcommon.Map, maxBytes int) int {
	changed := 0
	m.Range(func(_ string, v pcommon.Value) bool {
		if v.Type() != pcommon.ValueTypeStr {
			return true
		}
		s := v.Str()
		if len(s) <= maxBytes {
			return true
		}
		v.SetStr(s[:utf8SafeCut(s, maxBytes)])
		changed++
		return true
	})
	return changed
}

// utf8SafeCut returns the largest n <= maxBytes such that s[:n] ends on a
// UTF-8 codepoint boundary. A mid-codepoint cut produces invalid UTF-8 that
// strict encoders reject; we'd rather drop a few bytes than ship garbage.
// Caller has already established len(s) > maxBytes.
func utf8SafeCut(s string, maxBytes int) int {
	// Scan backward at most 3 bytes — UTF-8 codepoints are 1-4 bytes, so the
	// cut is at most 3 bytes before maxBytes.
	for n := maxBytes; n > maxBytes-4 && n > 0; n-- {
		if utf8.RuneStart(s[n]) {
			return n
		}
	}
	return maxBytes
}
