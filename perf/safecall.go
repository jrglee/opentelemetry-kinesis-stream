//go:build perf

package perf

import (
	"fmt"
)

// safeMarshal invokes a Marshal-style function and recovers any panic
// originating inside it, converting the panic to an error. Necessary
// because the upstream `arrow_record.Producer` panics on inputs it
// cannot handle (e.g. "Too many consecutive schema updates" at
// extreme cardinality), which would otherwise abort the entire
// benchmark sweep. A skipped case is a useful finding in its own right:
// the harness records the failure mode and moves on.
func safeMarshal[T any](fn func(T) ([]byte, error), in T) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(in)
}

func safeUnmarshal[T any](fn func([]byte) (T, error), in []byte) (out T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(in)
}
