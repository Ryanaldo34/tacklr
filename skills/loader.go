// Package skills discovers and parses application-owned SKILL.md files.
//
// SkillsPath is one directory. When the turn has a MountSession, that path
// is on the VFS. When it does not, the path is on the local machine.
// The agent reads instructions only through read_skill.
package skills

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs"
)

// DefaultRoot is the virtual directory the loader walks when Root is empty.
const DefaultRoot = vfs.WorkspacePoint + "/skills"

type Skill struct {
	Name         string
	Description  string
	Instructions string
	// Path is the virtual path of SKILL.md when loaded from a mount.
	Path string
}

// SkillLoader discovers skills for the harness. A loader owns its source
// configuration; callers only provide the lifetime context for the load.
type SkillLoader interface {
	Load(ctx context.Context) ([]Skill, error)
}

var _ SkillLoader = Loader{}

// Loader loads one skill per immediate child of Root.
// Session set means Root is a virtual path (empty Root is DefaultRoot).
// Session nil means Root is a local directory. An empty local Root loads nothing.
type Loader struct {
	Session *vfs.MountSession
	Root    string
}

// Load implements SkillLoader.
func (l Loader) Load(ctx context.Context) ([]Skill, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.Session == nil {
		return l.loadLocal(ctx)
	}
	dir := strings.TrimSpace(l.Root)
	if dir == "" {
		dir = DefaultRoot
	}

	entries, err := l.Session.Route(ctx, dir).
		ReadDir(ctx)
	if err != nil {
		if errors.Is(err, vfs.ErrNotMounted) || errors.Is(err, vfs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read skills directory %q: %w", dir, err)
	}
	slices.SortFunc(entries, func(a, b vfs.DirEntry) int { return cmp.Compare(a.Name, b.Name) })
	var loaded []Skill
	seen := map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir || entry.Type&fs.ModeSymlink != 0 {
			continue
		}
		skillPath := path.Join(dir, entry.Name, "SKILL.md")
		skill, err := readSkill(ctx, l.Session, skillPath, entry.Name)
		if err != nil {
			return nil, err
		}
		if seen[skill.Name] {
			return nil, fmt.Errorf("duplicate skill name %q", skill.Name)
		}
		seen[skill.Name] = true
		loaded = append(loaded, skill)
	}
	slices.SortFunc(loaded, func(a, b Skill) int { return cmp.Compare(a.Name, b.Name) })
	return loaded, nil
}

func (l Loader) loadLocal(ctx context.Context) ([]Skill, error) {
	dir := strings.TrimSpace(l.Root)
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read skills directory %q: %w", dir, err)
	}
	slices.SortFunc(entries, func(a, b os.DirEntry) int { return cmp.Compare(a.Name(), b.Name()) })
	var loaded []Skill
	seen := map[string]bool{}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		skillPath := filepath.Join(dir, entry.Name(), "SKILL.md")
		data, err := os.ReadFile(skillPath)
		if err != nil {
			return nil, fmt.Errorf("skill %q: read SKILL.md: %w", entry.Name(), err)
		}
		skill, err := parse(string(data))
		if err != nil {
			return nil, fmt.Errorf("skill %q: %w", entry.Name(), err)
		}
		if seen[skill.Name] {
			return nil, fmt.Errorf("duplicate skill name %q", skill.Name)
		}
		seen[skill.Name] = true
		skill.Path = skillPath
		loaded = append(loaded, skill)
	}
	slices.SortFunc(loaded, func(a, b Skill) int { return cmp.Compare(a.Name, b.Name) })
	return loaded, nil
}

func readSkill(ctx context.Context, ms *vfs.MountSession, skillPath, label string) (Skill, error) {
	data, err := ms.Route(ctx, skillPath).
		ReadFile(ctx)
	if err != nil {
		return Skill{}, fmt.Errorf("skill %q: read SKILL.md: %w", label, err)
	}
	skill, err := parse(string(data))
	if err != nil {
		return Skill{}, fmt.Errorf("skill %q: %w", label, err)
	}
	skill.Path = skillPath
	return skill, nil
}

func parse(document string) (Skill, error) {
	if !strings.HasPrefix(document, "---\n") {
		return Skill{}, fmt.Errorf("SKILL.md must start with front matter")
	}
	parts := strings.SplitN(document[4:], "\n---\n", 2)
	if len(parts) != 2 {
		return Skill{}, fmt.Errorf("SKILL.md has unterminated front matter")
	}
	metadata := map[string]string{}
	for _, line := range strings.Split(parts[0], "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			metadata[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	skill := Skill{
		Name:         metadata["name"],
		Description:  metadata["description"],
		Instructions: strings.TrimSpace(parts[1]),
	}
	if skill.Name == "" || skill.Description == "" || skill.Instructions == "" {
		return Skill{}, fmt.Errorf("name, description, and instructions are required")
	}
	return skill, nil
}

// Catalog creates the small prompt section shown before a skill is selected.
// Full instructions are intentionally omitted to preserve context window.
func Catalog(loaded []Skill) string {
	var b strings.Builder
	b.WriteString("Available skills:\n")
	for _, skill := range loaded {
		fmt.Fprintf(&b, "- %s: %s\n", skill.Name, skill.Description)
	}
	return b.String()
}
