package awskinesisreceiver

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// newKinesisClient mirrors the exporter's client construction. Kept separate
// so the components can be lifted into independent modules without an
// internal shared package becoming a coupling point.
func newKinesisClient(ctx context.Context, region, endpoint string) (*kinesis.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	opts := []func(*kinesis.Options){}
	if endpoint != "" {
		opts = append(opts, func(o *kinesis.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	return kinesis.NewFromConfig(cfg, opts...), nil
}
