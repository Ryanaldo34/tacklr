package skills

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
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

func mountSkills(t *testing.T, hosts ...string) *vfs.MountSession {
	t.Helper()
	reg := vfs.NewBackendRegistry()
	for i, host := range hosts {
		if err := os.MkdirAll(host, 0o755); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("pack%d", i)
		if err := reg.Register(vfs.LocalFactory{ID: id, Base: host, Skills: "."}); err != nil {
			t.Fatal(err)
		}
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	if err := ms.AttachSkills(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

func TestLoader_emptySessionAndMissingMount(t *testing.T) {
	loaded, err := (Loader{}).Load(context.Background())
	if err != nil || loaded != nil {
		t.Fatalf("nil session: loaded=%#v err=%v", loaded, err)
	}

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "work", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.Mount(t.Context(), vfs.MountSpec{Point: "/work", Profile: "work"}); err != nil {
		t.Fatal(err)
	}
	loaded, err = (Loader{Session: ms}).Load(t.Context())
	if err != nil || loaded != nil {
		t.Fatalf("no /skills: loaded=%#v err=%v", loaded, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Loader{Session: ms}).Load(ctx); err == nil {
		t.Fatal("expected canceled context error")
	}
}

func TestLoader_catalogAndFailurePaths(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "alpha", "alpha", "Alpha skill", "Do alpha things.")
	writeSkill(t, root, "beta", "beta", "Beta skill", "Do beta things.")
	if err := os.WriteFile(filepath.Join(root, "readme.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := (Loader{Session: mountSkills(t, root)}).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "beta" {
		t.Fatalf("loaded = %#v (want alpha, beta sorted)", loaded)
	}
	if loaded[0].Path != "/skills/alpha/SKILL.md" {
		t.Fatalf("path = %q", loaded[0].Path)
	}
	cat := Catalog(loaded)
	if !strings.Contains(cat, "- alpha: Alpha skill") || !strings.Contains(cat, "- beta: Beta skill") {
		t.Fatalf("catalog = %q", cat)
	}

	t.Run("unreadable pack", func(t *testing.T) {
		r := t.TempDir()
		writeSkill(t, r, "alpha", "alpha", "A", "Body.")
		ms := mountSkills(t, r)
		if err := os.Chmod(r, 0); err != nil {
			t.Skipf("chmod not supported: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(r, 0o755) })
		if _, err := (Loader{Session: ms}).Load(context.Background()); err == nil {
			t.Skip("unreadable dir still readable (e.g. root)")
		}
	})

	t.Run("missing skill md", func(t *testing.T) {
		r := t.TempDir()
		if err := os.Mkdir(filepath.Join(r, "empty"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := (Loader{Session: mountSkills(t, r)}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "SKILL.md") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("oversized skill file", func(t *testing.T) {
		r := t.TempDir()
		d := filepath.Join(r, "big")
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		huge := strings.Repeat("x", maxSkillFileSize+1)
		if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte(huge), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := (Loader{Session: mountSkills(t, r)}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("duplicate skill names across packs", func(t *testing.T) {
		r1, r2 := t.TempDir(), t.TempDir()
		writeSkill(t, r1, "a", "dup", "D1", "Body one.")
		writeSkill(t, r2, "b", "dup", "D2", "Body two.")
		if _, err := (Loader{Session: mountSkills(t, r1, r2)}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "duplicate") {
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
		if _, err := (Loader{Session: mountSkills(t, r)}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "front matter") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("symlink child directories are skipped", func(t *testing.T) {
		r := t.TempDir()
		writeSkill(t, r, "real", "real", "Real", "Instructions here.")
		if err := os.Symlink(filepath.Join(r, "real"), filepath.Join(r, "link")); err != nil {
			t.Skipf("symlink not supported: %v", err)
		}
		loaded, err := (Loader{Session: mountSkills(t, r)}).Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 1 || loaded[0].Name != "real" {
			t.Fatalf("expected only real skill, got %#v", loaded)
		}
	})

	t.Run("unreadable skill file after stat", func(t *testing.T) {
		r := t.TempDir()
		d := filepath.Join(r, "locked")
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(d, "SKILL.md")
		if err := os.WriteFile(path, []byte("---\nname: locked\ndescription: d\n---\n\nbody"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0); err != nil {
			t.Skipf("chmod not supported: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
		if _, err := (Loader{Session: mountSkills(t, r)}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "SKILL.md") {
			if err == nil {
				t.Skip("unreadable file still readable (e.g. root)")
			}
			t.Fatalf("err = %v", err)
		}
	})
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
