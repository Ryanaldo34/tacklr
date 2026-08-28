package builtins

import "github.com/ryanaldo34/tacklr/vfs"

// Default VFS backend constructors. Hosts call these; implementations live
// in package vfs.

var (
	Local           = vfs.Local
	Memory          = vfs.Memory
	S3              = vfs.S3
	Blob            = vfs.Blob
	Drive           = vfs.Drive
	DriveWith       = vfs.DriveWith
	Graph           = vfs.Graph
	Union           = vfs.Union
	NewGoogleDrive  = vfs.NewGoogleDrive
	NewGoogleDocs   = vfs.NewGoogleDocs
	NewGoogleSheets = vfs.NewGoogleSheets
	NewGraph        = vfs.NewGraph
)

// SDK adapters hosts pass to S3 and Blob.
type (
	AWSS3     = vfs.AWSS3
	AzureBlob = vfs.AzureBlob
)
