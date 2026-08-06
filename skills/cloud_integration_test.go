package skills

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	minioImage   = "minio/minio:RELEASE.2024-06-13T22-53-53Z"
	azuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.33.0"
)

func TestS3Loader_minio(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MinIO integration test in -short mode")
	}
	ctx := context.Background()
	container := runObjectStore(t, testcontainers.ContainerRequest{
		Image: minioImage,
		Env: map[string]string{
			"MINIO_ROOT_USER":     "minioadmin",
			"MINIO_ROOT_PASSWORD": "minioadmin",
		},
		Cmd:          []string{"server", "/data"},
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForHTTP("/minio/health/live").WithPort("9000/tcp"),
	})

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
	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	const bucket = "skills"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	putS3Skill(t, ctx, client, bucket, "skills/alpha/SKILL.md", "alpha", "Alpha", "Alpha instructions")
	putS3Skill(t, ctx, client, bucket, "skills/beta/SKILL.md", "beta", "Beta", "Beta instructions")

	loaded, err := S3Loader{Client: AWSS3Client{Client: client}, Bucket: bucket, Prefix: "skills/"}.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "beta" {
		t.Fatalf("loaded = %#v", loaded)
	}
}

func TestBlobLoader_azurite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Azurite integration test in -short mode")
	}
	ctx := context.Background()
	container := runObjectStore(t, testcontainers.ContainerRequest{
		Image:        azuriteImage,
		Cmd:          []string{"azurite", "--blobHost", "0.0.0.0", "--blobPort", "10000"},
		ExposedPorts: []string{"10000/tcp"},
		WaitingFor:   wait.ForListeningPort("10000/tcp"),
	})
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "10000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	connectionString := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=%s;BlobEndpoint=http://%s:%s/devstoreaccount1;",
		"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
		host, port.Port(),
	)
	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		t.Fatal(err)
	}
	const containerName = "skills"
	if _, err := client.CreateContainer(ctx, containerName, nil); err != nil {
		t.Fatal(err)
	}
	putBlobSkill(t, ctx, client, containerName, "skills/alpha/SKILL.md", "alpha", "Alpha", "Alpha instructions")
	putBlobSkill(t, ctx, client, containerName, "skills/beta/SKILL.md", "beta", "Beta", "Beta instructions")

	loaded, err := BlobLoader{Client: AzureBlobClient{Client: client}, Container: containerName, Prefix: "skills/"}.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "beta" {
		t.Fatalf("loaded = %#v", loaded)
	}
}

func runObjectStore(t *testing.T, request testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	if err != nil {
		t.Skipf("object storage emulator unavailable: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	return container
}

func putS3Skill(t *testing.T, ctx context.Context, client *s3.Client, bucket, key, name, description, instructions string) {
	t.Helper()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(skillDocument(name, description, instructions)),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func putBlobSkill(t *testing.T, ctx context.Context, client *azblob.Client, container, key, name, description, instructions string) {
	t.Helper()
	_, err := client.UploadBuffer(ctx, container, key, skillDocument(name, description, instructions), nil)
	if err != nil {
		t.Fatal(err)
	}
}

func skillDocument(name, description, instructions string) []byte {
	return []byte(fmt.Sprintf("---\nname: %s\ndescription: %s\n---\n\n%s", name, description, instructions))
}
