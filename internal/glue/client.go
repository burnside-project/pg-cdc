// Package glue registers Postgres tables in the AWS Glue Data Catalog so
// downstream consumers (pg-warehouse, Athena, Lake Formation tag policy)
// have a real catalog resource to target. pg-cdc owns the raw-layer schema
// and durability; this package owns the catalog registration step.
//
// Authentication uses the AWS SDK default credential chain, identical to
// the s3 sink adapter.
package glue

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
)

// Config holds Glue catalog connection settings.
type Config struct {
	Database string // Glue database name (e.g., "burnside_pg_cdc")
	Region   string // AWS region
	Endpoint string // custom endpoint for LocalStack / testing
}

// Adapter registers manifest tables in a Glue database.
type Adapter struct {
	client   *awsglue.Client
	database string
}

// New creates a Glue catalog adapter. Both database and region are required.
func New(cfg Config) (*Adapter, error) {
	if cfg.Database == "" {
		return nil, fmt.Errorf("glue: database is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("glue: region is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("glue: load aws config: %w", err)
	}

	client := awsglue.NewFromConfig(awsCfg, func(o *awsglue.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	return &Adapter{client: client, database: cfg.Database}, nil
}

// Database returns the Glue database the adapter writes to.
func (a *Adapter) Database() string {
	return a.database
}
