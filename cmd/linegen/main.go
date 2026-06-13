// Command linegen is a tiny deterministic InfluxDB line-protocol load
// generator for the metrics E2E. It cycles through a fixed (host, region,
// service) tag space, POSTing batches of line-protocol measurements to an
// InfluxDB v1 /write endpoint, then prints the total sent and the number of
// distinct (host, region) tuples so the test can assert no loss and tag
// locality. It depends on nothing outside the standard library.
package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "linegen:", err)
		os.Exit(1)
	}
}

func run() error {
	endpoint := env("INFLUX_ENDPOINT", "http://producer-metrics:8086")
	total := envInt("TOTAL", 2000)
	hosts := envInt("HOSTS", 8)
	regions := envInt("REGIONS", 3)
	services := envInt("SERVICES", 4)
	batch := envInt("BATCH", 200)
	if total <= 0 || hosts <= 0 || regions <= 0 || services <= 0 || batch <= 0 {
		return fmt.Errorf("TOTAL/HOSTS/REGIONS/SERVICES/BATCH must all be positive")
	}

	writeURL := strings.TrimRight(endpoint, "/") + "/write"
	regionNames := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south"}

	// Distinct (host, region) tuples actually emitted: the tag space cycles
	// deterministically, so after `total` measurements the reached set is the
	// smaller of total and the full host*region grid.
	distinct := map[[2]int]struct{}{}

	client := &http.Client{Timeout: 30 * time.Second}
	base := time.Now().UnixNano()

	var buf bytes.Buffer
	inBatch := 0
	flush := func() error {
		if inBatch == 0 {
			return nil
		}
		if err := post(client, writeURL, buf.Bytes()); err != nil {
			return err
		}
		buf.Reset()
		inBatch = 0
		return nil
	}

	for i := 0; i < total; i++ {
		h := i % hosts
		r := (i / hosts) % regions
		s := (i / (hosts * regions)) % services
		distinct[[2]int{h, r}] = struct{}{}

		region := regionNames[r%len(regionNames)]
		// Spread timestamps by 1ms so points are distinct in time.
		ts := base + int64(i)*int64(time.Millisecond)
		value := float64(i%100) / 100.0

		fmt.Fprintf(&buf, "system.cpu,host=h%d,region=%s,service=api%d value=%s %d\n",
			h, region, s, strconv.FormatFloat(value, 'f', 2, 64), ts)
		inBatch++

		if inBatch >= batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	// stdout contract consumed by the E2E: two prefixed lines it can parse.
	fmt.Printf("LINEGEN_SENT %d\n", total)
	fmt.Printf("LINEGEN_DISTINCT_TUPLES %d\n", len(distinct))
	return nil
}

func post(client *http.Client, url string, body []byte) error {
	resp, err := client.Post(url, "text/plain; charset=utf-8", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	// InfluxDB v1 /write returns 204 No Content on success.
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("write %s: status %d", url, resp.StatusCode)
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
