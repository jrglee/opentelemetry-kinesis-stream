// Command linegen is a tiny deterministic InfluxDB line-protocol load
// generator for the metrics E2E. It cycles through a fixed (host, region,
// device_id) tag space, POSTing batches of line-protocol measurements to an
// InfluxDB v1 /write endpoint. Measurement names each begin with a lowercase
// subsystem prefix separated by "_" (e.g. "charging_load", "network_rx"),
// so the exporter's ^([a-z]+)_ regex can extract a stable subsystem label.
//
// Stdout contract (parsed by the E2E test):
//
//	LINEGEN_SENT              <total>
//	LINEGEN_DISTINCT_DEVICES  <n>
//	LINEGEN_DISTINCT_SUBSYSTEMS <n>
//
// It depends on nothing outside the standard library.
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

// measurements cycles across two distinct subsystems (charging, network).
var measurements = []string{"charging_load", "charging_temp", "network_rx"}

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
	devices := envInt("DEVICES", 4)
	batch := envInt("BATCH", 200)
	if total <= 0 || hosts <= 0 || regions <= 0 || devices <= 0 || batch <= 0 {
		return fmt.Errorf("TOTAL/HOSTS/REGIONS/DEVICES/BATCH must all be positive")
	}

	writeURL := strings.TrimRight(endpoint, "/") + "/write"
	regionNames := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south"}

	// Track distinct device indices and subsystems actually emitted.
	distinctDevices := map[int]struct{}{}
	distinctSubsystems := map[string]struct{}{}

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
		d := i % devices
		measurement := measurements[i%len(measurements)]

		// Extract the subsystem prefix (everything before the first "_").
		subsystem := subsystemOf(measurement)

		distinctDevices[d] = struct{}{}
		distinctSubsystems[subsystem] = struct{}{}

		region := regionNames[r%len(regionNames)]
		// Spread timestamps by 1ms so points are distinct in time.
		ts := base + int64(i)*int64(time.Millisecond)
		value := float64(i%100) / 100.0

		// The InfluxDB receiver drops tag keys containing dots, so the per-producer
		// key is device_id here. The exporter's datapoint source reads whatever
		// attribute key the producer emits (a real OTLP producer would send
		// device.id); only this ingestion path constrains the spelling.
		fmt.Fprintf(&buf, "%s,host=h%d,region=%s,device_id=dev%d value=%s %d\n",
			measurement, h, region, d, strconv.FormatFloat(value, 'f', 2, 64), ts)
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

	// stdout contract consumed by the E2E: three prefixed lines it can parse.
	fmt.Printf("LINEGEN_SENT %d\n", total)
	fmt.Printf("LINEGEN_DISTINCT_DEVICES %d\n", len(distinctDevices))
	fmt.Printf("LINEGEN_DISTINCT_SUBSYSTEMS %d\n", len(distinctSubsystems))
	return nil
}

// subsystemOf returns the portion of name before the first "_", which is the
// subsystem prefix by convention (e.g. "charging" from "charging_load").
func subsystemOf(name string) string {
	if idx := strings.IndexByte(name, '_'); idx > 0 {
		return name[:idx]
	}
	return name
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
