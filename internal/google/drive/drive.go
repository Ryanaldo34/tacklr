// Package drive is the Drive v3 JSON the vfs adapter reads and writes.
package drive

import "time"

// File is a Drive file or folder.
type File struct {
	ID              string           `json:"id,omitempty"`
	Name            string           `json:"name,omitempty"`
	MimeType        string           `json:"mimeType,omitempty"`
	Size            int64            `json:"size,omitempty,string"`
	Version         int64            `json:"version,omitempty,string"`
	MD5Checksum     string           `json:"md5Checksum,omitempty"`
	ModifiedTime    time.Time        `json:"modifiedTime,omitzero"`
	ShortcutDetails *ShortcutDetails `json:"shortcutDetails,omitempty"`
	Parents         []string         `json:"parents,omitempty"`
	Trashed         bool             `json:"trashed,omitempty"`
}

// ShortcutDetails is the target of a shortcut file.
type ShortcutDetails struct {
	TargetID       string `json:"targetId,omitempty"`
	TargetMimeType string `json:"targetMimeType,omitempty"`
}

// FileList is one page of files.list.
type FileList struct {
	NextPageToken string  `json:"nextPageToken,omitempty"`
	Files         []*File `json:"files,omitempty"`
}
