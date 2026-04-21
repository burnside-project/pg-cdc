// Package s3 provides a ports.Sink adapter for Amazon S3.
//
// Authentication uses the AWS SDK default credential chain:
//   - EC2 / ECS: instance role or task role (automatic)
//   - EKS: IRSA via projected service account token (automatic)
//   - On-prem / laptop: AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY env vars,
//     ~/.aws/credentials, or AWS_PROFILE
//
// Uploads go through burnside-go's storage.S3, which streams large bodies
// (e.g. 800 MiB+ Parquet base files) via the S3 transfer manager without
// buffering the whole object in memory.
package s3

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/burnside-project/burnside-go/manifest"
	"github.com/burnside-project/burnside-go/storage"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Config holds S3-specific configuration.
type Config struct {
	Bucket   string // S3 bucket name
	Prefix   string // object key prefix (e.g., "cdc/prod/")
	Region   string // AWS region (e.g., "us-east-1")
	Endpoint string // custom endpoint for S3-compatible stores (MinIO, R2)
}

// Adapter implements ports.Sink for Amazon S3.
type Adapter struct {
	store *storage.S3
}

// Verify interface compliance at compile time.
var _ ports.Sink = (*Adapter)(nil)

// New creates an S3 sink adapter. It loads AWS credentials from the default
// chain and builds an S3 client; both the bucket and region are required.
// For MinIO / R2 / LocalStack, set Endpoint to the service URL.
func New(cfg Config) (*Adapter, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3: bucket is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("s3: region is required")
	}

	// Give the credential chain a bounded window; IMDS/SSO can hang otherwise.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}

	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			// MinIO and some on-prem gateways require path-style addressing.
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})

	return &Adapter{store: storage.NewS3(client, cfg.Bucket, cfg.Prefix)}, nil
}

func (a *Adapter) WriteFile(ctx context.Context, remotePath string, r io.Reader) error {
	return a.store.WriteFile(ctx, remotePath, r)
}

func (a *Adapter) ReadFile(ctx context.Context, path string) (io.ReadCloser, error) {
	return a.store.ReadFile(ctx, path)
}

func (a *Adapter) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	return a.store.ListFiles(ctx, prefix)
}

func (a *Adapter) DeleteFile(ctx context.Context, path string) error {
	return a.store.DeleteFile(ctx, path)
}

func (a *Adapter) ReadManifest(ctx context.Context) (*manifest.Manifest, error) {
	return a.store.ReadManifest(ctx)
}

func (a *Adapter) WriteManifest(ctx context.Context, m *manifest.Manifest) error {
	return a.store.WriteManifest(ctx, m)
}

func (a *Adapter) Backend() string {
	return "s3"
}
