// Package skills discovers and parses application-owned SKILL.md files: one
// skill per directory, each with a SKILL.md. Loader reads a local
// directory; package workspace reads one on the agent's workspace. The
// agent reads instructions only through read_skill.
package skills

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type Skill struct {
	Name         string
	Description  string
	Instructions string
	// Path is where SKILL.md was read from.
	Path string
}

// SkillLoader discovers skills for the harness. A loader owns its source
// configuration; callers only provide the lifetime context for the load.
type SkillLoader interface {
	Load(ctx context.Context) ([]Skill, error)
}

var _ SkillLoader = Loader{}

// Loader loads one skill per immediate child directory of the local
// directory Root. An empty Root, or one that does not exist, loads nothing.
// A skills directory in another store (the agent's workspace) uses Collect.
type Loader struct {
	Root string
}

// Load implements SkillLoader.
func (l Loader) Load(ctx context.Context) ([]Skill, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := strings.TrimSpace(l.Root)
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read skills directory %q: %w", dir, err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() && entry.Type()&fs.ModeSymlink == 0 {
			names = append(names, entry.Name())
		}
	}
	return Collect(ctx, names, func(_ context.Context, name string) ([]byte, string, error) {
		p := filepath.Join(dir, name, "SKILL.md")
		data, err := os.ReadFile(p)
		return data, p, err
	})
}

// Collect parses the SKILL.md of each skill directory in names, which read
// returns with the path it is at, and returns the skills sorted by name.
// Two skills with one name is an error.
func Collect(ctx context.Context, names []string, read func(ctx context.Context, name string) (data []byte, path string, err error)) ([]Skill, error) {
	loaded := make([]Skill, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, p, err := read(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("skill %q: read SKILL.md: %w", name, err)
		}
		skill, err := parse(string(data))
		if err != nil {
			return nil, fmt.Errorf("skill %q: %w", name, err)
		}
		if seen[skill.Name] {
			return nil, fmt.Errorf("duplicate skill name %q", skill.Name)
		}
		seen[skill.Name] = true
		skill.Path = p
		loaded = append(loaded, skill)
	}
	slices.SortFunc(loaded, func(a, b Skill) int { return cmp.Compare(a.Name, b.Name) })
	return loaded, nil
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
