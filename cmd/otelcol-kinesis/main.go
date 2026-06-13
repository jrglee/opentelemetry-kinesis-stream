// Command otelcol-kinesis is a custom OpenTelemetry Collector distribution
// wiring the Kinesis exporter and receiver alongside a minimal set of core
// and contrib components.
package main

import (
	"log"

	fileexporter "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/httpprovider"
	"go.opentelemetry.io/collector/confmap/provider/httpsprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/jrglee/opentelemetry-kinesis-stream/exporter/awskinesisexporter"
	"github.com/jrglee/opentelemetry-kinesis-stream/receiver/awskinesisreceiver"
)

func main() {
	info := component.BuildInfo{
		Command:     "otelcol-kinesis",
		Description: "Kinesis OTel Collector (PoC)",
		Version:     "0.0.0-dev",
	}
	settings := otelcol.CollectorSettings{
		BuildInfo: info,
		Factories: components,
		// A custom distribution must supply the config providers itself; the
		// otelcol assembly no longer defaults them. file/env/yaml/http(s) is
		// the standard set, and env is also what expands ${env:...} in configs.
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
					yamlprovider.NewFactory(),
					httpprovider.NewFactory(),
					httpsprovider.NewFactory(),
				},
			},
		},
	}
	if err := otelcol.NewCommand(settings).Execute(); err != nil {
		log.Fatal(err)
	}
}

func components() (otelcol.Factories, error) {
	var factories otelcol.Factories
	var err error

	if factories.Receivers, err = otelcol.MakeFactoryMap(
		otlpreceiver.NewFactory(),
		awskinesisreceiver.NewFactory(),
	); err != nil {
		return factories, err
	}

	if factories.Processors, err = otelcol.MakeFactoryMap(
		batchprocessor.NewFactory(),
	); err != nil {
		return factories, err
	}

	if factories.Exporters, err = otelcol.MakeFactoryMap(
		awskinesisexporter.NewFactory(),
		fileexporter.NewFactory(),
		debugexporter.NewFactory(),
	); err != nil {
		return factories, err
	}

	factories.Telemetry = otelconftelemetry.NewFactory()

	return factories, nil
}
