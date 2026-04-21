// Package gcs provides a ports.Sink adapter for Google Cloud Storage.
//
// Authentication uses Application Default Credentials (ADC):
//   - GCE VM: automatic via metadata server (no config needed)
//   - On-prem: GOOGLE_APPLICATION_CREDENTIALS env var or credentials_file config
//   - Cloud Run / GKE: Workload Identity (automatic)
//
// Requires: cloud.google.com/go/storage (added when this adapter is completed).
package gcs

import (
	"context"
	"fmt"
	"io"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Config holds GCS-specific configuration.
type Config struct {
	Bucket          string // GCS bucket name
	Prefix          string // object key prefix (e.g., "cdc/prod/")
	CredentialsFile string // path to service account JSON (optional, uses ADC if empty)
}

// Adapter implements ports.Sink for Google Cloud Storage.
type Adapter struct {
	cfg Config
}

// Verify interface compliance at compile time.
var _ ports.Sink = (*Adapter)(nil)

// New creates a GCS sink adapter.
func New(cfg Config) (*Adapter, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs: bucket is required")
	}
	return &Adapter{cfg: cfg}, nil
}

func (a *Adapter) WriteFile(ctx context.Context, remotePath string, r io.Reader) error {
	// TODO: implement with cloud.google.com/go/storage
	// key := a.cfg.Prefix + remotePath
	// w := client.Bucket(a.cfg.Bucket).Object(key).NewWriter(ctx)
	// io.Copy(w, r); w.Close()
	return fmt.Errorf("gcs sink: not yet implemented")
}

func (a *Adapter) ReadFile(ctx context.Context, path string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("gcs sink: not yet implemented")
}

func (a *Adapter) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	return nil, fmt.Errorf("gcs sink: not yet implemented")
}

func (a *Adapter) DeleteFile(ctx context.Context, path string) error {
	return fmt.Errorf("gcs sink: not yet implemented")
}

func (a *Adapter) ReadManifest(ctx context.Context) (*manifest.Manifest, error) {
	return nil, fmt.Errorf("gcs sink: not yet implemented")
}

func (a *Adapter) WriteManifest(ctx context.Context, m *manifest.Manifest) error {
	return fmt.Errorf("gcs sink: not yet implemented")
}

func (a *Adapter) Backend() string {
	return "gcs"
}
