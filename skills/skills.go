// Package skills is the agent's skills: methods the agent reads when a task
// needs one. A host configures a Provider (Dir reads SKILL.md files from a
// directory). The system prompt lists only each skill's name and
// description, and the agent reads a skill's instructions with read_skill,
// which asks the provider.
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

// Summary is a skill as the system prompt lists it.
type Summary struct {
	Name        string
	Description string
}

// Skill is a skill with its instructions.
type Skill struct {
	Name         string
	Description  string
	Instructions string
}

// Provider lists the agent's skills and reads one. It owns its source.
type Provider interface {
	// List returns each skill's name and description, sorted by name.
	List(ctx context.Context) ([]Summary, error)
	// Read returns the skill named name. It returns an error that wraps
	// ErrUnknown when the provider has no skill of that name.
	Read(ctx context.Context, name string) (Skill, error)
}

// ErrUnknown is the error of Provider.Read for a name it has no skill of.
var ErrUnknown = errors.New("skills: unknown skill")

// Dir is the skills of the local directory Root: one per child directory,
// with a SKILL.md (front matter with name and description, then the
// instructions). Files and symlinked directories are not skills. An empty
// Root, or one that does not exist, has no skills.
type Dir struct {
	Root string
}

// List implements Provider.
func (d Dir) List(ctx context.Context) ([]Summary, error) {
	loaded, err := d.load(ctx)
	summaries := make([]Summary, 0, len(loaded))
	for _, s := range loaded {
		summaries = append(summaries, Summary{Name: s.Name, Description: s.Description})
	}
	return summaries, err
}

// Read implements Provider.
func (d Dir) Read(ctx context.Context, name string) (Skill, error) {
	loaded, err := d.load(ctx)
	if err != nil {
		return Skill{}, err
	}
	i := slices.IndexFunc(loaded, func(s Skill) bool { return s.Name == name })
	if i < 0 {
		return Skill{}, fmt.Errorf("%w %q", ErrUnknown, name)
	}
	return loaded[i], nil
}

// load parses every skill under Root, sorted by name.
func (d Dir) load(ctx context.Context) ([]Skill, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := strings.TrimSpace(d.Root)
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
	var loaded []Skill
	seen := map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name(), "SKILL.md"))
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

// Catalog is the prompt section that lists the skills before one is read:
// names and descriptions only, to keep the window small.
func Catalog(summaries []Summary) string {
	var b strings.Builder
	b.WriteString("Available skills:\n")
	for _, s := range summaries {
		fmt.Fprintf(&b, "- %s: %s\n", s.Name, s.Description)
	}
	return b.String()
}
