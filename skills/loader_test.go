package skills

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSkill(t *testing.T, root, dir, name, desc, body string) {
	t.Helper()
	d := filepath.Join(root, dir)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	content := "---\nname: " + name + "\ndescription: " + desc + "\n---\n\n" + body
	if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoader_emptyMissingAndCanceled(t *testing.T) {
	loaded, err := (Loader{}).Load(context.Background())
	if err != nil || loaded != nil {
		t.Fatalf("empty root: loaded=%#v err=%v", loaded, err)
	}
	loaded, err = (Loader{Root: filepath.Join(t.TempDir(), "missing")}).Load(t.Context())
	if err != nil || loaded != nil {
		t.Fatalf("missing root: loaded=%#v err=%v", loaded, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Loader{Root: t.TempDir()}).Load(ctx); err == nil {
		t.Fatal("expected canceled context error")
	}
}

func TestLoader_catalogAndFailurePaths(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "beta", "beta", "Beta skill", "Do beta things.")
	writeSkill(t, root, "alpha", "alpha", "Alpha skill", "Do alpha things.")
	if err := os.WriteFile(filepath.Join(root, "readme.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := (Loader{Root: root}).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "beta" {
		t.Fatalf("loaded = %#v (want alpha, beta sorted)", loaded)
	}
	if loaded[0].Path != filepath.Join(root, "alpha", "SKILL.md") {
		t.Fatalf("path = %q", loaded[0].Path)
	}
	cat := Catalog(loaded)
	if !strings.Contains(cat, "- alpha: Alpha skill") || !strings.Contains(cat, "- beta: Beta skill") {
		t.Fatalf("catalog = %q", cat)
	}

	t.Run("unreadable pack", func(t *testing.T) {
		r := t.TempDir()
		writeSkill(t, r, "alpha", "alpha", "A", "Body.")
		if err := os.Chmod(r, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(r, 0o755) })
		if _, err := (Loader{Root: r}).Load(context.Background()); err == nil {
			t.Fatal("an unreadable skills dir loaded (tests must not run as root)")
		}
	})

	t.Run("missing skill md", func(t *testing.T) {
		r := t.TempDir()
		if err := os.Mkdir(filepath.Join(r, "empty"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := (Loader{Root: r}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "SKILL.md") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("duplicate skill names", func(t *testing.T) {
		r := t.TempDir()
		writeSkill(t, r, "a", "dup", "D1", "Body one.")
		writeSkill(t, r, "b", "dup", "D2", "Body two.")
		if _, err := (Loader{Root: r}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("parse failures surface from load", func(t *testing.T) {
		r := t.TempDir()
		d := filepath.Join(r, "bad")
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte("no front matter"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := (Loader{Root: r}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "front matter") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("symlink child directories are skipped", func(t *testing.T) {
		r := t.TempDir()
		writeSkill(t, r, "real", "real", "Real", "Instructions here.")
		if err := os.Symlink(filepath.Join(r, "real"), filepath.Join(r, "link")); err != nil {
			t.Fatal(err)
		}
		loaded, err := (Loader{Root: r}).Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 1 || loaded[0].Name != "real" {
			t.Fatalf("expected only real skill, got %#v", loaded)
		}
	})
}

// Collect stops at a canceled context between skills.
func TestCollect_stopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, err := Collect(ctx, []string{"a", "b"}, func(context.Context, string) ([]byte, string, error) {
		cancel()
		return []byte("---\nname: a\ndescription: d\n---\n\nbody"), "a", nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestParse_returnPaths(t *testing.T) {
	cases := []struct {
		name    string
		doc     string
		wantErr string
		want    Skill
	}{
		{
			name: "valid",
			doc:  "---\nname: weather\ndescription: Current conditions\n---\n\nUse the weather tool.",
			want: Skill{Name: "weather", Description: "Current conditions", Instructions: "Use the weather tool."},
		},
		{name: "missing front matter start", doc: "name: x\n---\nbody", wantErr: "front matter"},
		{name: "unterminated front matter", doc: "---\nname: x\ndescription: y\n", wantErr: "unterminated"},
		{name: "missing required fields", doc: "---\nname: only\n---\n\nbody", wantErr: "required"},
		{name: "empty instructions", doc: "---\nname: n\ndescription: d\n---\n\n   \n", wantErr: "required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parse(tc.doc)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %#v want %#v", got, tc.want)
			}
		})
	}
}
