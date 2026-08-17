package skills

import (
	"context"
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

func mountLocal(t *testing.T, point, host string) *vfs.MountSession {
	t.Helper()
	if err := os.MkdirAll(host, 0o755); err != nil {
		t.Fatal(err)
	}
	id := strings.TrimPrefix(point, "/")
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: id, Base: host}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	if err := ms.Mount(t.Context(), vfs.MountSpec{
		Point: point, Profile: id, ReadOnly: true, IndexPolicy: "none",
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

func mountLocals(t *testing.T, hosts map[string]string) *vfs.MountSession {
	t.Helper()
	reg := vfs.NewBackendRegistry()
	var specs []vfs.MountSpec
	for point, host := range hosts {
		if err := os.MkdirAll(host, 0o755); err != nil {
			t.Fatal(err)
		}
		id := strings.TrimPrefix(point, "/")
		if err := reg.Register(vfs.LocalFactory{ID: id, Base: host}); err != nil {
			t.Fatal(err)
		}
		specs = append(specs, vfs.MountSpec{
			Point: point, Profile: id, ReadOnly: true, IndexPolicy: "none",
		})
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	if err := ms.Materialize(t.Context(), specs); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

func TestLoader_unionMount(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	writeSkill(t, a, "alpha", "alpha", "Alpha skill", "Do alpha things.")
	writeSkill(t, b, "zeta", "zeta", "Zeta skill", "Do zeta things.")
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "a", Base: a}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.LocalFactory{ID: "b", Base: b}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	if err := ms.Mount(t.Context(), vfs.MountSpec{
		Point: "/skills", Profile: "skills", IndexPolicy: "none",
		Members: []vfs.MountSpec{{Profile: "a"}, {Profile: "b"}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	loaded, err := Loader{Session: ms, Roots: []string{"/skills"}}.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "zeta" {
		t.Fatalf("loaded = %#v", loaded)
	}
	if loaded[0].Path != "/skills/alpha/SKILL.md" || loaded[1].Path != "/skills/zeta/SKILL.md" {
		t.Fatalf("paths = %#v", loaded)
	}
}

func TestLoader_emptyRootsAndMissingSession(t *testing.T) {
	loaded, err := (Loader{}).Load(context.Background())
	if err != nil || loaded != nil {
		t.Fatalf("empty roots: loaded=%#v err=%v", loaded, err)
	}

	if _, err := (Loader{Roots: []string{"/skills"}}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "MountSession is required") {
		t.Fatalf("err = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Loader{Roots: []string{"/skills"}}).Load(ctx); err == nil {
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

	loaded, err := Loader{Session: mountLocal(t, "/skills", root), Roots: []string{"/skills"}}.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "beta" {
		t.Fatalf("loaded = %#v (want alpha, beta sorted)", loaded)
	}
	cat := Catalog(loaded)
	if !strings.Contains(cat, "- alpha: Alpha skill") || !strings.Contains(cat, "- beta: Beta skill") {
		t.Fatalf("catalog = %q", cat)
	}

	t.Run("missing root", func(t *testing.T) {
		ms := mountLocal(t, "/skills", t.TempDir())
		if _, err := (Loader{Session: ms, Roots: []string{"/missing"}}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "read skills directory") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("missing skill md", func(t *testing.T) {
		r := t.TempDir()
		if err := os.Mkdir(filepath.Join(r, "empty"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := (Loader{Session: mountLocal(t, "/skills", r), Roots: []string{"/skills"}}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "SKILL.md") {
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
		if _, err := (Loader{Session: mountLocal(t, "/skills", r), Roots: []string{"/skills"}}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("duplicate skill names across roots", func(t *testing.T) {
		r1, r2 := t.TempDir(), t.TempDir()
		writeSkill(t, r1, "a", "dup", "D1", "Body one.")
		writeSkill(t, r2, "b", "dup", "D2", "Body two.")
		ms := mountLocals(t, map[string]string{"/s1": r1, "/s2": r2})
		if _, err := (Loader{Session: ms, Roots: []string{"/s1", "/s2"}}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "duplicate") {
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
		if _, err := (Loader{Session: mountLocal(t, "/skills", r), Roots: []string{"/skills"}}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "front matter") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("symlink child directories are skipped", func(t *testing.T) {
		r := t.TempDir()
		writeSkill(t, r, "real", "real", "Real", "Instructions here.")
		target := filepath.Join(r, "real")
		link := filepath.Join(r, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Skipf("symlink not supported: %v", err)
		}
		loaded, err := Loader{Session: mountLocal(t, "/skills", r), Roots: []string{"/skills"}}.Load(context.Background())
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
		if _, err := (Loader{Session: mountLocal(t, "/skills", r), Roots: []string{"/skills"}}).Load(context.Background()); err == nil || !strings.Contains(err.Error(), "SKILL.md") {
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
