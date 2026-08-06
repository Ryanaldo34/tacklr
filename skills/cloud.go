package skills

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// AWSS3Client adapts the AWS SDK S3 client to S3Client. Listing consumes all
// pages before returning so SkillLoader sees a complete source.
type AWSS3Client struct {
	Client *s3.Client
}

func (c AWSS3Client) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("skills: AWS S3 client is required")
	}
	pager := s3.NewListObjectsV2Paginator(c.Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			if object.Key != nil {
				keys = append(keys, *object.Key)
			}
		}
	}
	return keys, nil
}

func (c AWSS3Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("skills: AWS S3 client is required")
	}
	object, err := c.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return object.Body, nil
}

// AzureBlobClient adapts the Azure Blob SDK client to BlobClient.
type AzureBlobClient struct {
	Client *azblob.Client
}

func (c AzureBlobClient) ListBlobs(ctx context.Context, container, prefix string) ([]string, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("skills: Azure Blob client is required")
	}
	options := &azblob.ListBlobsFlatOptions{}
	if prefix != "" {
		options.Prefix = &prefix
	}
	pager := c.Client.NewListBlobsFlatPager(container, options)
	var names []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, blob := range page.Segment.BlobItems {
			if blob.Name != nil {
				names = append(names, *blob.Name)
			}
		}
	}
	return names, nil
}

func (c AzureBlobClient) DownloadBlob(ctx context.Context, container, name string) (io.ReadCloser, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("skills: Azure Blob client is required")
	}
	object, err := c.Client.DownloadStream(ctx, container, name, nil)
	if err != nil {
		return nil, err
	}
	return object.Body, nil
}
