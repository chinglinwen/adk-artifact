# adk-artifact

Artifact services (`fsartifact`, `s3artifact`) implementing `artifact.Service` from
[`google.golang.org/adk/v2`](https://github.com/google/adk-go) (tested with v2.6.0, requires Go 1.26.6+).

```go
import "google.golang.org/adk/v2/artifact"
```

The examples below also use ADK v2 packages, e.g. `google.golang.org/adk/v2/session/database`
and `google.golang.org/adk/v2/model/gemini`.

## fs artifact

```go
import(
	"github.com/chinglinwen/adk-artifact/fsartifact"
)

func main(){
    // --- 1. Setup Services ---

	// Session Service (SQLite)
	dbPath := "adk_session.db"
	dialector := sqlite.Open(dbPath)
	sessionService, err := database.NewSessionService(dialector)
	if err != nil {
		log.Fatalf("failed to create session service: %s", err)
	}
	if err := database.AutoMigrate(sessionService); err != nil {
		log.Fatalf("failed to auto migrate session db: %s", err)
	}

	// Artifact Service (Local FS)
	artifactsDir := "adk_artifacts"
	artService, err := fsartifact.NewService(artifactsDir)
	if err != nil {
		log.Fatalf("failed to create fs artifact service: %s", err)
	}

	model, err := gemini.NewModel(ctx, "gemini-2.5-flash", &genai.ClientConfig{})
	if err != nil {
		log.Fatalf("failed to create model: %s", err)
	}

	...
}
```

## s3 artifact

```go

import(
	"github.com/chinglinwen/adk-artifact/s3artifact"
)

func main(){
	// --- 1. Setup Services ---

	// Session Service (SQLite)
	dbPath := "adk_session.db"
	dialector := sqlite.Open(dbPath)
	sessionService, err := database.NewSessionService(dialector)
	if err != nil {
		log.Fatalf("failed to create session service: %s", err)
	}
	if err := database.AutoMigrate(sessionService); err != nil {
		log.Fatalf("failed to auto migrate session db: %s", err)
	}

	// Artifact Service (Local S3)
	// You need a local S3-compatible service running (e.g., SeaweedFS or MinIO).
	endpoint := "http://localhost:8333"
	accessKey := "admin"
	secretKey := "secret"
	bucketName := "test-bucket"

	artService, err := s3artifact.NewService(ctx, bucketName,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               endpoint,
				SigningRegion:     "us-east-1",
				HostnameImmutable: true,
			}, nil
		})),
	)
	if err != nil {
		log.Printf("Warning: failed to connect to local S3 artifact service: %s. Artifact logging might fail.", err)
		// We don't fatal here to allow running without S3 if not available, though the example intends to use it.
		// If strict behavior is desired, change to log.Fatalf
	}

	model, err := gemini.NewModel(ctx, "gemini-2.5-flash", &genai.ClientConfig{})
	if err != nil {
		log.Fatalf("failed to create model: %s", err)
	}

	...
}
```