// Package s3test starts the S3-compatible server live tests run against.
package s3test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Image is an S3-compatible server. MinIO no longer publishes images.
const Image = "rustfs/rustfs:1.0.4"

const key, secret = "tacklr", "tacklr-secret"

// Start starts a server for t, creates bucket on it, and returns a
// path-style client.
func Start(ctx context.Context, t testing.TB, bucket string) *s3.Client {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithEnv(map[string]string{"RUSTFS_ACCESS_KEY": key, "RUSTFS_SECRET_KEY": secret}),
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/health").WithPort("9000/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("S3 server: %v", err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, "9000/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(fmt.Errorf("create bucket %s: %w", bucket, err))
	}
	return client
}
