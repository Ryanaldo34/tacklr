package vfs

// MountInfo is a host- and agent-safe description of a mount.
// It never includes host roots, bucket names, credentials, backend type, or
// other source config — only what the virtual namespace itself exposes.
type MountInfo struct {
	// Point is the absolute virtual mount path (e.g. "/data").
	Point string
	// ReadOnly is true when the mount was created read-only.
	ReadOnly bool
}
