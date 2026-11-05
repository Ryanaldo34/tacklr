// Package s3test runs the S3-compatible server live tests share. Every test
// package of one go test run shares one server: a reusable container by
// name, which Ryuk removes when the run ends. Each test gets a bucket of its
// own.
package s3test

import (
	"context"
	"crypto/rand"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/internal/faultproxy"
)

// Image is an S3-compatible server. MinIO no longer publishes images.
const Image = "rustfs/rustfs:1.0.4"

const key, secret = "tacklr", "tacklr-secret"

var (
	serverOnce sync.Once
	serverURL  *url.URL
	serverErr  error
)

// server returns the shared server's base URL, starting it on first use.
func server(t testing.TB) *url.URL {
	t.Helper()
	serverOnce.Do(func() {
		ctx := context.Background()
		ctr, err := testcontainers.Run(ctx, Image,
			testcontainers.WithEnv(map[string]string{"RUSTFS_ACCESS_KEY": key, "RUSTFS_SECRET_KEY": secret}),
			testcontainers.WithExposedPorts("9000/tcp"),
			testcontainers.WithWaitStrategy(wait.ForHTTP("/health").WithPort("9000/tcp")),
			testcontainers.WithReuseByName("tacklr-s3-"+testcontainers.SessionID()),
		)
		if err != nil {
			serverErr = err
			return
		}
		var endpoint string
		if endpoint, serverErr = ctr.PortEndpoint(ctx, "9000/tcp", "http"); serverErr == nil {
			serverURL, serverErr = url.Parse(endpoint)
		}
	})
	if serverErr != nil {
		t.Fatalf("S3 server: %v", serverErr)
	}
	return serverURL
}

// client is a path-style client for the server at endpoint. A client of the
// server retries: the server answers 503 for a moment after it starts.
// A client through a proxy does not, so a fault the proxy injects reaches
// the caller once.
func client(endpoint string, retryer aws.Retryer) *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(key, secret, ""),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Retryer:      retryer,
	})
}

// Start returns a client of the shared server and a new bucket on it that
// no other test uses.
func Start(t testing.TB) (*s3.Client, string) {
	t.Helper()
	c := client(server(t).String(), retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 10 }))
	bucket := "t-" + strings.ToLower(rand.Text())
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
	return c, bucket
}

// Proxy starts a fault proxy in front of the shared server and returns it
// with a client that goes through it.
func Proxy(t testing.TB) (*faultproxy.Proxy, *s3.Client) {
	t.Helper()
	p := faultproxy.Start(t, server(t))
	return p, client(p.URL(), aws.NopRetryer{})
}
