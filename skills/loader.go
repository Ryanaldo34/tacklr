// Package skills discovers and parses application-owned SKILL.md files.
//
// Discovery walks /skills on a vfs.MountSession. Hosts mark backends with
// LocalFactory.Skills / S3Factory.Skills / BlobFactory.Skills; the session attaches the union.
package skills

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs"
)

const maxSkillFileSize = 1024 * 1024

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

// Loader loads one skill per immediate child of /skills.
// A nil session or missing /skills mount loads nothing.
type Loader struct {
	Session *vfs.MountSession
}

// Load implements SkillLoader.
func (l Loader) Load(ctx context.Context) ([]Skill, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.Session == nil {
		return nil, nil
	}
	entries, err := l.Session.ReadDir(ctx, vfs.SkillsPoint)
	if err != nil {
		if errors.Is(err, vfs.ErrNotMounted) {
			return nil, nil
		}
		return nil, fmt.Errorf("read skills directory %q: %w", vfs.SkillsPoint, err)
	}
	slices.SortFunc(entries, func(a, b vfs.DirEntry) int { return cmp.Compare(a.Name, b.Name) })
	var loaded []Skill
	seen := map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir || entry.Type&fs.ModeSymlink != 0 {
			continue
		}
		skillPath := path.Join(vfs.SkillsPoint, entry.Name, "SKILL.md")
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

func readSkill(ctx context.Context, ms *vfs.MountSession, skillPath, label string) (Skill, error) {
	info, err := ms.Stat(ctx, skillPath)
	if err != nil {
		return Skill{}, fmt.Errorf("skill %q: read SKILL.md: %w", label, err)
	}
	if info.Size > maxSkillFileSize {
		return Skill{}, fmt.Errorf("skill %q: SKILL.md exceeds %d bytes", label, maxSkillFileSize)
	}
	data, err := ms.ReadFile(ctx, skillPath)
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
	b.WriteString("Available skills (use read_skill to load instructions):\n")
	for _, skill := range loaded {
		fmt.Fprintf(&b, "- %s: %s\n", skill.Name, skill.Description)
	}
	return b.String()
}
