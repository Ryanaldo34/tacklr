package skills_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/skills"
)

// writeSkillMD writes doc as root/dir/SKILL.md.
func writeSkillMD(t *testing.T, root, dir, doc string) {
	t.Helper()
	d := filepath.Join(root, dir)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

func skillMD(name, description, body string) string {
	return "---\nname: " + name + "\ndescription: " + description + "\n---\n\n" + body
}

// TestLoader_loadsEachSkillDirectorySortedByName: a pack loads one skill
// per real child directory, sorted by name, with its path and instructions;
// files and symlinked directories are not skills. The catalog lists each
// skill's name and description. An empty or missing root loads no skills.
func TestLoader_loadsEachSkillDirectorySortedByName(t *testing.T) {
	root := t.TempDir()
	writeSkillMD(t, root, "beta", skillMD("beta", "Beta skill", "Do beta things."))
	writeSkillMD(t, root, "alpha", skillMD("alpha", "Alpha skill", "Do alpha things.\n"))
	if err := os.WriteFile(filepath.Join(root, "readme.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "alpha"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	loaded, err := skills.Loader{Root: root}.Load(t.Context())
	empty, emptyErr := skills.Loader{Root: " "}.Load(t.Context())
	missing, missingErr := skills.Loader{Root: filepath.Join(root, "missing")}.Load(t.Context())

	if err != nil || len(loaded) != 2 {
		t.Fatalf("load = %#v %v, want alpha and beta", loaded, err)
	}
	if alpha := loaded[0]; alpha.Name != "alpha" || alpha.Instructions != "Do alpha things." || alpha.Path != filepath.Join(root, "alpha", "SKILL.md") {
		t.Fatalf("first skill = %#v, want alpha with its instructions and path", alpha)
	}
	if cat := skills.Catalog(loaded); cat != "Available skills:\n- alpha: Alpha skill\n- beta: Beta skill\n" {
		t.Fatalf("catalog = %q", cat)
	}
	if empty != nil || emptyErr != nil || missing != nil || missingErr != nil {
		t.Fatalf("empty root = %#v %v, missing root = %#v %v, want no skills", empty, emptyErr, missing, missingErr)
	}
}

// TestLoader_failsOnAPackItCannotLoad: each pack that cannot load fails
// with an error that names the cause.
func TestLoader_failsOnAPackItCannotLoad(t *testing.T) {
	cases := map[string]struct {
		docs map[string]string // dir -> SKILL.md; "" makes the dir only
		want string
	}{
		"a directory without SKILL.md": {map[string]string{"empty": ""}, "read SKILL.md"},
		"two skills of one name":       {map[string]string{"a": skillMD("dup", "D1", "One."), "b": skillMD("dup", "D2", "Two.")}, `duplicate skill name "dup"`},
		"no front matter":              {map[string]string{"bad": "name: x\n---\nbody"}, "must start with front matter"},
		"unterminated front matter":    {map[string]string{"bad": "---\nname: x\ndescription: y\n"}, "unterminated front matter"},
		"no description":               {map[string]string{"bad": "---\nname: only\n---\n\nbody"}, "are required"},
		"no instructions":              {map[string]string{"bad": skillMD("n", "d", "   \n")}, "are required"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			for dir, doc := range tc.docs {
				if doc == "" {
					if err := os.Mkdir(filepath.Join(root, dir), 0o755); err != nil {
						t.Fatal(err)
					}
					continue
				}
				writeSkillMD(t, root, dir, doc)
			}

			_, err := skills.Loader{Root: root}.Load(t.Context())

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("load = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestLoader_failsOnADirectoryItCannotReadOrAnEndedContext: an unreadable
// skills directory fails the load, and a context that ends stops a load or
// a Collect between two skills with the context's error.
func TestLoader_failsOnADirectoryItCannotReadOrAnEndedContext(t *testing.T) {
	unreadable := t.TempDir()
	writeSkillMD(t, unreadable, "alpha", skillMD("alpha", "A", "Body."))
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o755) })
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	between, cancelBetween := context.WithCancel(t.Context())

	_, readErr := skills.Loader{Root: unreadable}.Load(t.Context())
	_, loadErr := skills.Loader{Root: t.TempDir()}.Load(ended)
	_, collectErr := skills.Collect(between, []string{"a", "b"}, func(context.Context, string) ([]byte, string, error) {
		cancelBetween()
		return []byte(skillMD("a", "d", "body")), "a", nil
	})

	if readErr == nil || !strings.Contains(readErr.Error(), "read skills directory") {
		t.Fatalf("load of an unreadable directory = %v, want a read error (tests must not run as root)", readErr)
	}
	if !errors.Is(loadErr, context.Canceled) || !errors.Is(collectErr, context.Canceled) {
		t.Fatalf("load = %v, collect = %v, want context.Canceled", loadErr, collectErr)
	}
}
