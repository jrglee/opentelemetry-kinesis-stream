package awskinesisreceiver

import (
	"testing"
	"time"
)

// TestStuckBackoff pins the exponential backoff schedule used when the poll loop
// is wedged on a downstream that keeps rejecting the head record: it grows from
// PollInterval and caps at stuckBackoffMax, so the loop stops hammering the
// Kinesis read path instead of re-reading every PollInterval.
func TestStuckBackoff(t *testing.T) {
	p := &shardPoller{cfg: &Config{PollInterval: 100 * time.Millisecond}}

	cases := []struct {
		passes int
		want   time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
	}
	for _, c := range cases {
		if got := p.stuckBackoff(c.passes); got != c.want {
			t.Fatalf("stuckBackoff(%d): got %v want %v", c.passes, got, c.want)
		}
	}

	// A long stall caps at stuckBackoffMax and never overflows past it.
	if got := p.stuckBackoff(100); got != stuckBackoffMax {
		t.Fatalf("stuckBackoff(100): got %v want cap %v", got, stuckBackoffMax)
	}
	for passes := 0; passes < 64; passes++ {
		if got := p.stuckBackoff(passes); got > stuckBackoffMax || got <= 0 {
			t.Fatalf("stuckBackoff(%d) out of range: %v", passes, got)
		}
	}
}
