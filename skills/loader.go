// Package skills discovers and parses application-owned SKILL.md files.
package skills

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const maxSkillFileSize = 1024 * 1024

type Skill struct {
	Name         string
	Description  string
	Instructions string
}

// Loader discovers skills for the harness. Hosts can inject a custom Loader via
// AgentOptions.SkillsLoader (for example to load from object storage).
type Loader interface {
	Load(directories []string) ([]Skill, error)
}

// DirectoryLoader loads one skill per immediate child directory under each root.
// It is the default when AgentOptions.SkillsLoader is nil.
type DirectoryLoader struct{}

// Load implements Loader using LoadDirectories.
func (DirectoryLoader) Load(directories []string) ([]Skill, error) {
	return LoadDirectories(directories)
}

// LoadDirectories loads one skill per immediate child directory. Directories
// are processed in lexical order and duplicate skill names are rejected.
func LoadDirectories(roots []string) ([]Skill, error) {
	var loaded []Skill
	seen := map[string]bool{}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, fmt.Errorf("read skills directory %q: %w", root, err)
		}
		for _, entry := range entries {
			if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			path := filepath.Join(root, entry.Name(), "SKILL.md")
			info, err := os.Stat(path)
			if err != nil {
				return nil, fmt.Errorf("skill %q: read SKILL.md: %w", entry.Name(), err)
			}
			if info.Size() > maxSkillFileSize {
				return nil, fmt.Errorf("skill %q: SKILL.md exceeds %d bytes", entry.Name(), maxSkillFileSize)
			}
			data, err := os.ReadFile(path)
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
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].Name < loaded[j].Name })
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
	b.WriteString("Available skills (use read_skill to load instructions):\n")
	for _, skill := range loaded {
		fmt.Fprintf(&b, "- %s: %s\n", skill.Name, skill.Description)
	}
	return b.String()
}
