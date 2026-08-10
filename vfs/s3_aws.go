package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// AWSS3 adapts *s3.Client to S3API for S3Factory.Client.
type AWSS3 struct {
	Client *s3.Client
}

func (a AWSS3) require() error {
	if a.Client == nil {
		return fmt.Errorf("vfs: AWS S3 client required")
	}
	return nil
}

// Head implements S3API.
func (a AWSS3) Head(ctx context.Context, bucket, key string) (int64, time.Time, error) {
	if err := a.require(); err != nil {
		return 0, time.Time{}, err
	}
	out, err := a.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, time.Time{}, mapS3Error(err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	var mod time.Time
	if out.LastModified != nil {
		mod = *out.LastModified
	}
	return size, mod, nil
}

// Get implements S3API.
func (a AWSS3) Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, time.Time, error) {
	if err := a.require(); err != nil {
		return nil, 0, time.Time{}, err
	}
	out, err := a.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, time.Time{}, mapS3Error(err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	var mod time.Time
	if out.LastModified != nil {
		mod = *out.LastModified
	}
	return out.Body, size, mod, nil
}

// Put implements S3API.
func (a AWSS3) Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
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

// Delete implements S3API.
func (a AWSS3) Delete(ctx context.Context, bucket, key string) error {
	if err := a.require(); err != nil {
		return err
	}
	_, err := a.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return mapS3Error(err)
}

// List implements S3API with delimiter "/" for virtual directories.
func (a AWSS3) List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error) {
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
		return ErrNotExist
	}
	var nsb *types.NotFound
	if errors.As(err, &nsb) {
		return ErrNotExist
	}
	// HeadObject 404 / NoSuchKey often wrap as generic API errors.
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "notfound") || strings.Contains(msg, "no such key") || strings.Contains(msg, "status code: 404") {
		return ErrNotExist
	}
	return err
}
