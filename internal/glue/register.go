package glue

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/burnside-project/burnside-go/manifest"
)

// tableAPI is the subset of the Glue client used during registration.
// Narrowing it makes register testable without standing up a real client.
type tableAPI interface {
	CreateTable(ctx context.Context, in *awsglue.CreateTableInput, opts ...func(*awsglue.Options)) (*awsglue.CreateTableOutput, error)
	UpdateTable(ctx context.Context, in *awsglue.UpdateTableInput, opts ...func(*awsglue.Options)) (*awsglue.UpdateTableOutput, error)
}

// Register catalogues every active table in the manifest. CreateTable is
// attempted first; on AlreadyExistsException the table is updated instead.
// The operation is idempotent: running it twice with the same manifest is
// indistinguishable from running it once.
//
// locationFor maps a manifest key ("schema.table") to a fully-qualified
// storage URI (e.g., "s3://bucket/prefix/public.users.db/"). The caller
// owns this mapping because pg-cdc supports filesystem / GCS / S3 sinks.
func (a *Adapter) Register(ctx context.Context, m *manifest.Manifest, locationFor func(string) string) error {
	return register(ctx, a.client, a.database, m, locationFor)
}

func register(ctx context.Context, api tableAPI, database string, m *manifest.Manifest, locationFor func(string) string) error {
	if m == nil {
		return fmt.Errorf("glue: manifest is nil")
	}
	if locationFor == nil {
		return fmt.Errorf("glue: locationFor is nil")
	}

	for qualified, mt := range m.Tables {
		if mt.Status != "active" {
			continue
		}
		ms, ok := m.Schemas[qualified]
		if !ok {
			return fmt.Errorf("glue: no schema entry for %q", qualified)
		}

		input, err := tableInput(qualified, mt, ms, locationFor(qualified))
		if err != nil {
			return err
		}

		if err := upsert(ctx, api, database, input); err != nil {
			return fmt.Errorf("glue: register %s: %w", qualified, err)
		}
	}
	return nil
}

func upsert(ctx context.Context, api tableAPI, database string, input *gluetypes.TableInput) error {
	_, err := api.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String(database),
		TableInput:   input,
	})
	if err == nil {
		return nil
	}

	var alreadyExists *gluetypes.AlreadyExistsException
	if errors.As(err, &alreadyExists) || strings.Contains(err.Error(), "AlreadyExistsException") {
		_, uerr := api.UpdateTable(ctx, &awsglue.UpdateTableInput{
			DatabaseName: aws.String(database),
			TableInput:   input,
		})
		return uerr
	}
	return err
}