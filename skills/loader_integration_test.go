package skills

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/vfs"
)

const minioImage = "minio/minio:RELEASE.2024-06-13T22-53-53Z"

func TestLoader_s3Mount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MinIO integration test in -short mode")
	}
	ctx := context.Background()
	client, bucket := startMinIO(ctx, t)

	put := func(key, body string) {
		t.Helper()
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(body),
		}); err != nil {
			t.Fatal(err)
		}
	}
	put("pack/alpha/SKILL.md", "---\nname: alpha\ndescription: A\n---\n\nA body")
	put("pack/zeta/SKILL.md", "---\nname: zeta\ndescription: Z\n---\n\nZ body")
	put("pack/alpha/readme", "ignored")

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.S3Factory{
		ID:            "s3",
		Client:        vfs.AWSS3{Client: client},
		DefaultBucket: bucket,
		Skills:        "pack",
	}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("skills-s3", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.AttachSkills(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	loaded, err := (Loader{Session: ms}).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "zeta" {
		t.Fatalf("loaded = %#v", loaded)
	}
	if loaded[0].Path != "/skills/alpha/SKILL.md" || loaded[1].Instructions != "Z body" {
		t.Fatalf("loaded = %#v", loaded)
	}
}

func startMinIO(ctx context.Context, t *testing.T) (*s3.Client, string) {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: minioImage,
			Env: map[string]string{
				"MINIO_ROOT_USER":     "minioadmin",
				"MINIO_ROOT_PASSWORD": "minioadmin",
			},
			Cmd:          []string{"server", "/data"},
			ExposedPorts: []string{"9000/tcp"},
			WaitingFor:   wait.ForHTTP("/minio/health/live").WithPort("9000/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	const bucket = "skills"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	return client, bucket
}
