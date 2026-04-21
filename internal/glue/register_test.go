package glue

import (
	"context"
	"errors"
	"testing"

	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/burnside-project/burnside-go/manifest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeGlue struct {
	created []string
	updated []string
	createErr error
}

func (f *fakeGlue) CreateTable(_ context.Context, in *awsglue.CreateTableInput, _ ...func(*awsglue.Options)) (*awsglue.CreateTableOutput, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.created = append(f.created, *in.TableInput.Name)
	return &awsglue.CreateTableOutput{}, nil
}

func (f *fakeGlue) UpdateTable(_ context.Context, in *awsglue.UpdateTableInput, _ ...func(*awsglue.Options)) (*awsglue.UpdateTableOutput, error) {
	f.updated = append(f.updated, *in.TableInput.Name)
	return &awsglue.UpdateTableOutput{}, nil
}

func sampleManifest() *manifest.Manifest {
	return &manifest.Manifest{
		Tables: map[string]manifest.Table{
			"public.users":    {Status: "active", Path: "public.users.db/"},
			"public.sessions": {Status: "excluded"},
		},
		Schemas: map[string]manifest.Schema{
			"public.users": {
				Version: 1,
				Columns: []manifest.Column{
					{Name: "id", ParquetType: "INT64"},
				},
			},
		},
	}
}

func TestRegister_CreateNew(t *testing.T) {
	api := &fakeGlue{}
	loc := func(q string) string { return "s3://bucket/" + q + "/" }

	err := register(context.Background(), api, "db", sampleManifest(), loc)
	require.NoError(t, err)

	assert.Equal(t, []string{"users"}, api.created, "only active tables registered")
	assert.Empty(t, api.updated)
}

func TestRegister_FallsBackToUpdate(t *testing.T) {
	api := &fakeGlue{createErr: &gluetypes.AlreadyExistsException{}}
	loc := func(q string) string { return "s3://bucket/" + q + "/" }

	err := register(context.Background(), api, "db", sampleManifest(), loc)
	require.NoError(t, err)

	assert.Empty(t, api.created)
	assert.Equal(t, []string{"users"}, api.updated, "AlreadyExists triggers UpdateTable")
}

func TestRegister_PropagatesUnexpectedError(t *testing.T) {
	api := &fakeGlue{createErr: errors.New("AccessDeniedException: no perms")}
	loc := func(q string) string { return "s3://bucket/" + q + "/" }

	err := register(context.Background(), api, "db", sampleManifest(), loc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccessDenied")
}

func TestRegister_NilGuards(t *testing.T) {
	api := &fakeGlue{}
	require.Error(t, register(context.Background(), api, "db", nil, func(string) string { return "" }))
	require.Error(t, register(context.Background(), api, "db", sampleManifest(), nil))
}

func TestRegister_MissingSchema(t *testing.T) {
	api := &fakeGlue{}
	m := &manifest.Manifest{
		Tables:  map[string]manifest.Table{"public.users": {Status: "active"}},
		Schemas: map[string]manifest.Schema{},
	}
	err := register(context.Background(), api, "db", m, func(string) string { return "s3://x/" })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no schema entry")
}