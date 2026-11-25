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

// TestDir_listsEachSkillDirectoryAndReadsOne: a directory lists one skill
// per real child directory, sorted by name, by name and description; files
// and symlinked directories are not skills. Read returns a skill's
// instructions, and a name with no skill is unknown. The catalog lists each
// skill's name and description. An empty or missing root has no skills.
func TestDir_listsEachSkillDirectoryAndReadsOne(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeSkillMD(t, root, "beta", skillMD("beta", "Beta skill", "Do beta things."))
	writeSkillMD(t, root, "alpha", skillMD("alpha", "Alpha skill", "Do alpha things.\n"))
	if err := os.WriteFile(filepath.Join(root, "readme.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "alpha"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	dir := skills.Dir{Root: root}

	listed, listErr := dir.List(t.Context())
	alpha, readErr := dir.Read(t.Context(), "alpha")
	_, unknownErr := dir.Read(t.Context(), "gamma")
	empty, emptyErr := skills.Dir{Root: " "}.List(t.Context())
	missing, missingErr := skills.Dir{Root: filepath.Join(root, "missing")}.List(t.Context())

	if listErr != nil || len(listed) != 2 || listed[0] != (skills.Summary{Name: "alpha", Description: "Alpha skill"}) {
		t.Fatalf("list = %#v %v, want alpha then beta", listed, listErr)
	}
	if readErr != nil || alpha.Instructions != "Do alpha things." {
		t.Fatalf("read alpha = %#v %v, want its instructions", alpha, readErr)
	}
	if !errors.Is(unknownErr, skills.ErrUnknown) {
		t.Fatalf("read gamma = %v, want ErrUnknown", unknownErr)
	}
	if cat := skills.Catalog(listed); cat != "Available skills:\n- alpha: Alpha skill\n- beta: Beta skill\n" {
		t.Fatalf("catalog = %q", cat)
	}
	if len(empty) != 0 || emptyErr != nil || len(missing) != 0 || missingErr != nil {
		t.Fatalf("empty root = %#v %v, missing root = %#v %v, want no skills", empty, emptyErr, missing, missingErr)
	}
}

// TestDir_failsOnADirectoryItCannotLoad: each directory that cannot load
// fails with an error that names the cause.
func TestDir_failsOnADirectoryItCannotLoad(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
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

			_, err := skills.Dir{Root: root}.Read(t.Context(), "x")

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("read = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestDir_failsOnADirectoryItCannotReadOrAnEndedContext: an unreadable
// skills directory fails the list, and an ended context stops it with the
// context's error.
func TestDir_failsOnADirectoryItCannotReadOrAnEndedContext(t *testing.T) {
	t.Parallel()
	unreadable := t.TempDir()
	writeSkillMD(t, unreadable, "alpha", skillMD("alpha", "A", "Body."))
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o755) })
	ended, cancel := context.WithCancel(t.Context())
	cancel()

	_, readErr := skills.Dir{Root: unreadable}.List(t.Context())
	_, endedErr := skills.Dir{Root: t.TempDir()}.List(ended)

	if readErr == nil || !strings.Contains(readErr.Error(), "read skills directory") {
		t.Fatalf("list of an unreadable directory = %v, want a read error (tests must not run as root)", readErr)
	}
	if !errors.Is(endedErr, context.Canceled) {
		t.Fatalf("list = %v, want context.Canceled", endedErr)
	}
}
