package awskinesisexporter

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// newKinesisClient builds a Kinesis client honoring an optional endpoint
// override. SDK v2 reads credentials from the default chain (env, shared
// config, IAM role); emulators that need dummy credentials should set them
// via environment.
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
