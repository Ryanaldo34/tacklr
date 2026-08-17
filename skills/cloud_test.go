package skills

import (
	"context"
	"strings"
	"testing"
)

func TestAWSS3Client_requiresClient(t *testing.T) {
	client := AWSS3Client{}
	ctx := context.Background()

	if _, err := client.ListObjects(ctx, "bucket", "prefix"); err == nil || !strings.Contains(err.Error(), "AWS S3 client is required") {
		t.Fatalf("ListObjects error = %v", err)
	}
	if _, err := client.GetObject(ctx, "bucket", "key"); err == nil || !strings.Contains(err.Error(), "AWS S3 client is required") {
		t.Fatalf("GetObject error = %v", err)
	}
}

func TestAzureBlobClient_requiresClient(t *testing.T) {
	client := AzureBlobClient{}
	ctx := context.Background()

	if _, err := client.ListBlobs(ctx, "container", "prefix"); err == nil || !strings.Contains(err.Error(), "Azure Blob client is required") {
		t.Fatalf("ListBlobs error = %v", err)
	}
	if _, err := client.DownloadBlob(ctx, "container", "name"); err == nil || !strings.Contains(err.Error(), "Azure Blob client is required") {
		t.Fatalf("DownloadBlob error = %v", err)
	}
}
