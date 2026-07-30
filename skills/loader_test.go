package skills

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	skill, err := parse("---\nname: weather\ndescription: Current conditions\n---\n\nUse the weather tool.")
	if err != nil {
		t.Fatal(err)
	}
	if skill.Name != "weather" || skill.Description != "Current conditions" || skill.Instructions != "Use the weather tool." {
		t.Fatalf("unexpected skill: %#v", skill)
	}
}

func TestLoadDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "weather"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "weather", "SKILL.md"), []byte("---\nname: weather\ndescription: Conditions\n---\nUse get_weather."), 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadDirectories([]string{root})
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].Name != "weather" {
		t.Fatalf("unexpected skills: %#v", loaded)
	}
}

func TestCatalog_listsNameAndDescription(t *testing.T) {
	got := Catalog([]Skill{
		{Name: "a", Description: "Alpha"},
		{Name: "b", Description: "Beta"},
	})
	if !strings.Contains(got, "Available skills") {
		t.Fatalf("missing preamble: %q", got)
	}
	if !strings.Contains(got, "- a: Alpha") || !strings.Contains(got, "- b: Beta") {
		t.Fatalf("catalog = %q", got)
	}
}
