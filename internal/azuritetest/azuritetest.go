// Package azuritetest runs the Azurite blob server live tests share. Each
// test binary starts one server, which Ryuk removes when the binary ends,
// and each test gets a container of its own.
package azuritetest

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/internal/faultproxy"
)

// Image is the Azurite server.
const Image = "mcr.microsoft.com/azure-storage/azurite:3.33.0"

const (
	account = "devstoreaccount1"
	key     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

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
			testcontainers.WithCmd("azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--skipApiVersionCheck"),
			testcontainers.WithExposedPorts("10000/tcp"),
			testcontainers.WithWaitStrategy(wait.ForListeningPort("10000/tcp")),
		)
		if err != nil {
			serverErr = err
			return
		}
		var endpoint string
		if endpoint, serverErr = ctr.PortEndpoint(ctx, "10000/tcp", "http"); serverErr == nil {
			serverURL, serverErr = url.Parse(endpoint)
		}
	})
	if serverErr != nil {
		t.Fatalf("Azurite: %v", serverErr)
	}
	return serverURL
}

// client is a client of the Azurite account at endpoint, the server or a
// proxy in front of it, that tries a request up to retries+1 times. A
// client through a proxy does not retry, so a fault the proxy injects
// reaches the caller once.
func client(t testing.TB, endpoint string, retries int32) *azblob.Client {
	t.Helper()
	conn := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;",
		account, key, endpoint, account)
	c, err := azblob.NewClientFromConnectionString(conn, &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: retries, RetryDelay: 100 * time.Millisecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Start returns a client of the shared server and a new container on it
// that no other test uses.
func Start(t testing.TB) (*azblob.Client, string) {
	t.Helper()
	c := client(t, server(t).String(), 8)
	name := "t" + strings.ToLower(rand.Text())
	if _, err := c.CreateContainer(t.Context(), name, nil); err != nil {
		t.Fatalf("create container %s: %v", name, err)
	}
	return c, name
}

// Proxy starts a fault proxy in front of the shared server and returns it
// with a client that goes through it.
func Proxy(t testing.TB) (*faultproxy.Proxy, *azblob.Client) {
	t.Helper()
	p := faultproxy.Start(t, server(t))
	return p, client(t, p.URL(), -1)
}
