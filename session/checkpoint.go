package session

import (
	"cmp"
	"crypto/sha256"
	"maps"
	"slices"
)

// CheckpointEntry is one row of a session's checkpoint: one message of the
// window, one todo, the plan, one user-state key, one permission decision
// or one search rest. Kind names what it is and Key which one; only the
// harness reads Value. A commit writes only the rows it changed.
type CheckpointEntry struct {
	Kind  string `json:"kind"`
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value"`
}

// CheckpointChange is one change of a Save: a put of the entry, or with
// Delete the removal of the row its Kind and Key name.
type CheckpointChange struct {
	CheckpointEntry
	Delete bool `json:"delete,omitempty"`
}

// CheckpointRow names one row of a checkpoint.
type CheckpointRow struct {
	Kind, Key string
}

// CheckpointDigest is what a runtime keeps of the checkpoint it last saved
// or loaded: a hash of each row's value, so the next commit can tell which
// rows changed without holding their bytes.
type CheckpointDigest map[CheckpointRow][sha256.Size]byte

// DigestOf is the digest of entries.
func DigestOf(entries []CheckpointEntry) CheckpointDigest {
	d := make(CheckpointDigest, len(entries))
	for _, e := range entries {
		d[CheckpointRow{e.Kind, e.Key}] = sha256.Sum256(e.Value)
	}
	return d
}

// Diff is the changes that take a checkpoint whose digest is prev to next:
// a put of each row that is new or changed, in next's order, then a delete
// of each row next does not have, in row order. It returns next's digest.
func Diff(prev CheckpointDigest, next []CheckpointEntry) ([]CheckpointChange, CheckpointDigest) {
	digest := DigestOf(next)
	var changes []CheckpointChange
	for _, e := range next {
		row := CheckpointRow{e.Kind, e.Key}
		if had, ok := prev[row]; !ok || had != digest[row] {
			changes = append(changes, CheckpointChange{CheckpointEntry: e})
		}
	}
	gone := slices.SortedFunc(maps.Keys(prev), func(a, b CheckpointRow) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Key, b.Key))
	})
	for _, row := range gone {
		if _, kept := digest[row]; !kept {
			changes = append(changes, CheckpointChange{CheckpointEntry: CheckpointEntry{Kind: row.Kind, Key: row.Key}, Delete: true})
		}
	}
	return changes, digest
}
