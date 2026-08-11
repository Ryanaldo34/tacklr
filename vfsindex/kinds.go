package vfsindex

import "github.com/ryanaldo34/tacklr/brain"

// Property keys written on mount-indexed objects.
const (
	PropVFSPath     = "vfs_path"
	PropSize        = "size"
	PropMTime       = "mtime"
	PropContentHash = "content_hash"
	PropMediaType   = "media_type"
	PropStartLine   = "start_line"
	PropEndLine     = "end_line"
	PropByteStart   = "byte_start"
	PropByteEnd     = "byte_end"
	PropBlockID     = "block_id"
	PropHeadingPath = "heading_path"
)

// Default kind names (host may override on MountIndexer).
const (
	DefaultDocumentKind = "Document"
	DefaultChunkKind    = "Chunk"
)

// MountIndexKinds returns KindSpecs for catalog mode when indexing mounts.
// Fields are optional so pure knowledge Documents/Chunks without vfs_path remain valid.
func MountIndexKinds() []brain.KindSpec {
	return []brain.KindSpec{
		{
			Kind:        DefaultDocumentKind,
			Description: "Parent document (knowledge or VFS-indexed file)",
			IsParent:    true,
			Fields: []brain.FieldSpec{
				{Name: PropVFSPath, Type: brain.FieldTypeString, Description: "Virtual path when indexed from a mount"},
				{Name: PropSize, Type: brain.FieldTypeNumber},
				{Name: PropMTime, Type: brain.FieldTypeDateTime},
				{Name: PropContentHash, Type: brain.FieldTypeString},
				{Name: PropMediaType, Type: brain.FieldTypeString},
			},
		},
		{
			Kind:        DefaultChunkKind,
			Description: "Content chunk (part of a Document)",
			IsPart:      true,
			Fields: []brain.FieldSpec{
				{Name: PropStartLine, Type: brain.FieldTypeNumber},
				{Name: PropEndLine, Type: brain.FieldTypeNumber},
				{Name: PropByteStart, Type: brain.FieldTypeNumber},
				{Name: PropByteEnd, Type: brain.FieldTypeNumber},
				{Name: PropBlockID, Type: brain.FieldTypeString, Description: "Structured block id when indexed from IR"},
				{Name: PropHeadingPath, Type: brain.FieldTypeString},
			},
		},
	}
}
