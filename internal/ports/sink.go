package ports

import (
	"context"
	"io"

	"github.com/burnside-project/burnside-go/manifest"
)

// Sink is the outbound port for writing files to cloud storage.
// Adapters exist for filesystem (via burnside-go), GCS, and S3.
type Sink interface {
	// WriteFile uploads content to the given remote path.
	WriteFile(ctx context.Context, remotePath string, r io.Reader) error

	// ReadFile downloads a file from storage.
	ReadFile(ctx context.Context, path string) (io.ReadCloser, error)

	// ListFiles lists files under a prefix.
	ListFiles(ctx context.Context, prefix string) ([]string, error)

	// DeleteFile removes a file from storage.
	DeleteFile(ctx context.Context, path string) error

	// ReadManifest reads and parses the manifest from storage.
	ReadManifest(ctx context.Context) (*manifest.Manifest, error)

	// WriteManifest serialises and writes the manifest to storage.
	WriteManifest(ctx context.Context, m *manifest.Manifest) error

	// Backend returns the sink type name (e.g., "filesystem", "gcs", "s3").
	Backend() string
}