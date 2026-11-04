// Package workspace is the agent's /workspace on a turn: the tree of mounts
// the host chose (package vfs), and what the harness gets from it. That is
// the read and write tools, index_file and unindex into the brain, the
// skills directory, and a place to keep results trimmed from the window.
//
// The harness core does not import this package. A Linux worker host wires
// it in (worker.Config.Workspace); a host without one has no workspace.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// DefaultSkillsPath is where skills load from when Config.SkillsPath is
// empty.
const DefaultSkillsPath = mount.WorkspacePoint + "/skills"

// Config is an agent's workspace.
type Config struct {
	// Open builds the turn's tree. Nil: no workspace.
	Open vfs.OpenVFS
	// SkillsPath is the workspace directory the agent's skills load from
	// when the agent has no SkillsLoader. Empty is DefaultSkillsPath.
	SkillsPath string
	// OverflowPath is a directory on a writable mount (for example an
	// object-storage mount) that keeps the full text of results trimmed
	// from the window, at <OverflowPath>/<session>/<call id>.txt. Empty:
	// the full text is not kept.
	OverflowPath string
	// IndexReaders resolves a file's source ACL when index_file indexes it
	// (see vfsindex.MountIndexer.Readers). Nil indexes files visible to the
	// whole SearchNamespace.
	IndexReaders vfsindex.ReadersFunc
}

// Bind returns opts for a turn whose workspace is ms: its Tools, its
// IndexTools when opts has a Brain, its skills directory unless opts has a
// SkillsLoader, and its Overflow when OverflowPath is set.
func (c Config) Bind(opts tacklr.AgentOptions, ms *vfs.MountSession) (tacklr.AgentOptions, error) {
	opts.Tools = append(slices.Clone(opts.Tools), Tools(ms)...)
	if opts.Brain != nil {
		opts.Tools = append(opts.Tools, IndexTools(&vfsindex.MountIndexer{
			VFS:     ms,
			Brain:   opts.Brain,
			Scope:   brain.Scope{Namespace: opts.SearchNamespace.Clone(), Principals: slices.Clone(opts.SearchPrincipals)},
			Readers: c.IndexReaders,
		})...)
	}
	if opts.SkillsLoader == nil {
		opts.SkillsLoader = Skills(ms, c.SkillsPath)
	}
	if c.OverflowPath != "" {
		o, err := Overflow(ms, c.OverflowPath)
		if err != nil {
			return opts, err
		}
		opts.Overflow = o
	}
	return opts, nil
}

// Tools are read, write, write_document and write_spreadsheet on ms.
func Tools(ms *vfs.MountSession) []*tacklr.Tool {
	v := vfsTools{ms: ms}
	return []*tacklr.Tool{v.newRead(), v.newWrite(), v.newWriteDocument(), v.newWriteSpreadsheet()}
}

// IndexTools are index_file and unindex, which put workspace files into
// the brain through ix and take them out.
func IndexTools(ix *vfsindex.MountIndexer) []*tacklr.Tool {
	v := vfsIndexTools{idx: ix}
	return []*tacklr.Tool{v.newIndexFile(), v.newUnindex()}
}

// Skills loads one skill per directory under dir on ms (empty:
// DefaultSkillsPath). A missing directory has no skills.
func Skills(ms *vfs.MountSession, dir string) skills.SkillLoader {
	if dir == "" {
		dir = DefaultSkillsPath
	}
	return skillsDir{ms: ms, dir: dir}
}

type skillsDir struct {
	ms  *vfs.MountSession
	dir string
}

func (s skillsDir) Load(ctx context.Context) ([]skills.Skill, error) {
	entries, err := s.ms.Route(ctx, s.dir).ReadDir(ctx)
	if errors.Is(err, vfs.ErrNotMounted) || errors.Is(err, vfs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read skills directory %q: %w", s.dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir && e.Type&fs.ModeSymlink == 0 {
			names = append(names, e.Name)
		}
	}
	return skills.Collect(ctx, names, func(ctx context.Context, name string) ([]byte, string, error) {
		p := path.Join(s.dir, name, "SKILL.md")
		data, err := s.ms.Route(ctx, p).ReadFile(ctx)
		return data, p, err
	})
}

// Overflow keeps trimmed results under dir, a directory on a writable
// mount of ms, where the agent reads them with read.
func Overflow(ms *vfs.MountSession, dir string) (tacklr.Overflow, error) {
	spec, err := ms.SpecAt(dir)
	if err != nil {
		return nil, fmt.Errorf("workspace: OverflowPath %q: %w", dir, err)
	}
	if spec.ReadOnly {
		return nil, fmt.Errorf("workspace: OverflowPath %q is on a read-only mount", dir)
	}
	return overflow{ms: ms, dir: dir}, nil
}

type overflow struct {
	ms  *vfs.MountSession
	dir string
}

func (o overflow) Keep(ctx context.Context, sessionID, callID, text string) (string, error) {
	dir := path.Join(o.dir, sessionID)
	if err := o.ms.Route(ctx, dir).MkdirAll(ctx); err != nil {
		return "", err
	}
	p := path.Join(dir, callID+".txt")
	if err := o.ms.Route(ctx, p).WriteFile(ctx, []byte(text)); err != nil {
		return "", err
	}
	return p, nil
}
