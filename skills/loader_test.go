package skills

import (
	"context"
	"io"
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

// TestDirectoryLoader_implementsLoader verifies the injectable SkillLoader path.
func TestDirectoryLoader_implementsLoader(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "alpha", "alpha", "Alpha skill", "Do alpha things.")
	var loader SkillLoader = DirectoryLoader{Directories: []string{root}}
	loaded, err := loader.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].Name != "alpha" {
		t.Fatalf("loaded = %#v", loaded)
	}
}

type objectFixture struct {
	objects map[string]string
}

func (f objectFixture) ListObjects(context.Context, string, string) ([]string, error) {
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	return keys, nil
}

func (f objectFixture) GetObject(_ context.Context, _, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(f.objects[key])), nil
}

func (f objectFixture) ListBlobs(context.Context, string, string) ([]string, error) {
	return f.ListObjects(context.Background(), "", "")
}

func (f objectFixture) DownloadBlob(_ context.Context, _, key string) (io.ReadCloser, error) {
	return f.GetObject(context.Background(), "", key)
}

func TestObjectLoaders_loadAndFilterSkillObjects(t *testing.T) {
	objects := objectFixture{objects: map[string]string{
		"skills/zeta/SKILL.md":  "---\nname: zeta\ndescription: Z\n---\n\nZ body",
		"skills/alpha/SKILL.md": "---\nname: alpha\ndescription: A\n---\n\nA body",
		"skills/alpha/readme":   "ignored",
		"other/beta/SKILL.md":   "ignored",
	}}

	loaders := []SkillLoader{
		S3Loader{Client: objects, Bucket: "bucket", Prefix: "skills/"},
		BlobLoader{Client: objects, Container: "container", Prefix: "skills/"},
	}
	for _, loader := range loaders {
		loaded, err := loader.Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "zeta" {
			t.Fatalf("loaded = %#v", loaded)
		}
	}
}

func TestCloudAdapters_requireSDKClients(t *testing.T) {
	ctx := context.Background()
	if _, err := (AWSS3Client{}).ListObjects(ctx, "b", ""); err == nil {
		t.Fatal("nil AWS list")
	}
	if _, err := (AWSS3Client{}).GetObject(ctx, "b", "k"); err == nil {
		t.Fatal("nil AWS get")
	}
	if _, err := (AzureBlobClient{}).ListBlobs(ctx, "c", ""); err == nil {
		t.Fatal("nil Azure list")
	}
	if _, err := (AzureBlobClient{}).DownloadBlob(ctx, "c", "n"); err == nil {
		t.Fatal("nil Azure download")
	}
}

func TestS3Loader_rejectsOversizedObject(t *testing.T) {
	loader := S3Loader{
		Client: objectFixture{objects: map[string]string{
			"skills/big/SKILL.md": strings.Repeat("x", maxSkillFileSize+1),
		}},
		Bucket: "bucket",
		Prefix: "skills/",
	}
	if _, err := loader.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v", err)
	}
}

// TestLoadDirectories_catalogAndFailurePaths loads a multi-skill tree (happy
// path + catalog) and asserts each documented failure return as its own outcome.
func TestLoadDirectories_catalogAndFailurePaths(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "alpha", "alpha", "Alpha skill", "Do alpha things.")
	writeSkill(t, root, "beta", "beta", "Beta skill", "Do beta things.")
	// Non-dir entries are skipped.
	if err := os.WriteFile(filepath.Join(root, "readme.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadDirectories([]string{root})
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
		if _, err := LoadDirectories([]string{filepath.Join(root, "nope")}); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("missing skill md", func(t *testing.T) {
		r := t.TempDir()
		if err := os.Mkdir(filepath.Join(r, "empty"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadDirectories([]string{r}); err == nil || !strings.Contains(err.Error(), "SKILL.md") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("oversized skill file", func(t *testing.T) {
		r := t.TempDir()
		d := filepath.Join(r, "big")
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		// maxSkillFileSize is 1MiB; write slightly more.
		huge := strings.Repeat("x", maxSkillFileSize+1)
		if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte(huge), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadDirectories([]string{r}); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("duplicate skill names across roots", func(t *testing.T) {
		r1, r2 := t.TempDir(), t.TempDir()
		writeSkill(t, r1, "a", "dup", "D1", "Body one.")
		writeSkill(t, r2, "b", "dup", "D2", "Body two.")
		if _, err := LoadDirectories([]string{r1, r2}); err == nil || !strings.Contains(err.Error(), "duplicate") {
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
		if _, err := LoadDirectories([]string{r}); err == nil || !strings.Contains(err.Error(), "front matter") {
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
		loaded, err := LoadDirectories([]string{r})
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
		// Also need dir executable for Stat of file in some setups.
		if _, err := LoadDirectories([]string{r}); err == nil || !strings.Contains(err.Error(), "SKILL.md") {
			// If running as root, chmod 0 may still allow read — skip then.
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
