// Package awss3 connects the vfs S3 provider to the AWS SDK v2 S3 client:
//
//	vfs.S3(awss3.API{Client: client}, bucket)
package awss3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/internal/httprange"
)

// API is a vfs.S3API over the AWS SDK v2 client (AWS S3, MinIO, and the S3
// endpoint of R2).
type API struct {
	Client *s3.Client
}

func (a API) require() error {
	if a.Client == nil {
		return fmt.Errorf("awss3: client required")
	}
	return nil
}

// Head implements vfs.S3API.
func (a API) Head(ctx context.Context, bucket, key string) (vfs.S3Object, error) {
	if err := a.require(); err != nil {
		return vfs.S3Object{}, err
	}
	out, err := a.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return vfs.S3Object{}, mapS3Error(err)
	}
	return vfs.S3Object{
		Size:        aws.ToInt64(out.ContentLength),
		ModTime:     aws.ToTime(out.LastModified),
		ContentType: aws.ToString(out.ContentType),
		ETag:        aws.ToString(out.ETag),
	}, nil
}

// Get implements vfs.S3API.
func (a API) Get(ctx context.Context, bucket, key string, off, n int64) (io.ReadCloser, error) {
	if err := a.require(); err != nil {
		return nil, err
	}
	in := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if r := httprange.Header(off, n); r != "" {
		in.Range = aws.String(r)
	}
	out, err := a.Client.GetObject(ctx, in)
	if err != nil {
		return nil, mapS3Error(err)
	}
	return out.Body, nil
}

// Put implements vfs.S3API.
func (a API) Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
	if err := a.require(); err != nil {
		return err
	}
	in := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if size >= 0 {
		in.ContentLength = aws.Int64(size)
	}
	_, err := a.Client.PutObject(ctx, in)
	return mapS3Error(err)
}

// Delete implements vfs.S3API.
func (a API) Delete(ctx context.Context, bucket, key string) error {
	if err := a.require(); err != nil {
		return err
	}
	_, err := a.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return mapS3Error(err)
}

// List implements vfs.S3API with delimiter "/" for virtual directories.
func (a API) List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error) {
	if err := a.require(); err != nil {
		return nil, nil, err
	}
	pager := s3.NewListObjectsV2Paginator(a.Client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, mapS3Error(err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		for _, p := range page.CommonPrefixes {
			if p.Prefix != nil {
				dirs = append(dirs, *p.Prefix)
			}
		}
	}
	return keys, dirs, nil
}

func mapS3Error(err error) error {
	if err == nil {
		return nil
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return vfs.ErrNotExist
	}
	var nsb *types.NotFound
	if errors.As(err, &nsb) {
		return vfs.ErrNotExist
	}
	// HeadObject 404 / NoSuchKey often wrap as generic API errors.
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "notfound") || strings.Contains(msg, "no such key") || strings.Contains(msg, "status code: 404") {
		return vfs.ErrNotExist
	}
	return err
}
