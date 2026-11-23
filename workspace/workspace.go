// Package workspace is what the harness gets from the agent's /workspace on
// a turn (package vfs): the skills directory. The agent reads and writes the workspace's files
// with run_command (package sandbox).
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

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
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
}

// Bind returns opts for a turn whose workspace is ms: its skills directory
// unless opts has a SkillsLoader.
func (c Config) Bind(opts tacklr.AgentOptions, ms *vfs.MountSession) tacklr.AgentOptions {
	if opts.SkillsLoader == nil {
		opts.SkillsLoader = Skills(ms, c.SkillsPath)
	}
	return opts
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
