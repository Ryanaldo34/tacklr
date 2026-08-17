// Package skills discovers and parses application-owned SKILL.md files.
package skills

import (
	"context"
	"fmt"
	"io"
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

// SkillLoader discovers skills for the harness. A loader owns its source
// configuration; callers only provide the lifetime context for the load.
type SkillLoader interface {
	Load(ctx context.Context) ([]Skill, error)
}

// DirectoryLoader loads one skill per immediate child directory under each
// root. It is the default when AgentOptions.SkillsLoader is nil.
type DirectoryLoader struct {
	Directories []string
}

// Load implements SkillLoader using LoadDirectories.
func (l DirectoryLoader) Load(ctx context.Context) ([]Skill, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return LoadDirectories(l.Directories)
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

// S3Client is the subset of an S3 client required by S3Loader. Implementations
// can delegate to an SDK client and keep SDK-specific request types out of the
// skills package.
type S3Client interface {
	ListObjects(ctx context.Context, bucket, prefix string) ([]string, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

// S3Loader loads SKILL.md objects from an S3-compatible bucket.
type S3Loader struct {
	Client S3Client
	Bucket string
	Prefix string
}

// Load implements SkillLoader.
func (l S3Loader) Load(ctx context.Context) ([]Skill, error) {
	if l.Client == nil {
		return nil, fmt.Errorf("skills: S3 client is required")
	}
	return loadObjects(ctx, l.Prefix, func(ctx context.Context, key string) (io.ReadCloser, error) {
		return l.Client.GetObject(ctx, l.Bucket, key)
	}, func(ctx context.Context) ([]string, error) {
		return l.Client.ListObjects(ctx, l.Bucket, l.Prefix)
	})
}

// BlobClient is the subset of an Azure Blob Storage client required by
// BlobLoader. Implementations can delegate to an Azure SDK client.
type BlobClient interface {
	ListBlobs(ctx context.Context, container, prefix string) ([]string, error)
	DownloadBlob(ctx context.Context, container, name string) (io.ReadCloser, error)
}

// BlobLoader loads SKILL.md blobs from an Azure Blob Storage container.
type BlobLoader struct {
	Client    BlobClient
	Container string
	Prefix    string
}

// Load implements SkillLoader.
func (l BlobLoader) Load(ctx context.Context) ([]Skill, error) {
	if l.Client == nil {
		return nil, fmt.Errorf("skills: Blob client is required")
	}
	return loadObjects(ctx, l.Prefix, func(ctx context.Context, key string) (io.ReadCloser, error) {
		return l.Client.DownloadBlob(ctx, l.Container, key)
	}, func(ctx context.Context) ([]string, error) {
		return l.Client.ListBlobs(ctx, l.Container, l.Prefix)
	})
}

func loadObjects(
	ctx context.Context,
	prefix string,
	read func(context.Context, string) (io.ReadCloser, error),
	list func(context.Context) ([]string, error),
) ([]Skill, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys, err := list(ctx)
	if err != nil {
		return nil, fmt.Errorf("list skills objects: %w", err)
	}
	sort.Strings(keys)
	loaded := make([]Skill, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !isSkillObject(prefix, key) {
			continue
		}
		body, err := readSkillObject(ctx, key, read)
		if err != nil {
			return nil, err
		}
		skill, err := parse(string(body))
		if err != nil {
			return nil, fmt.Errorf("skill %q: %w", key, err)
		}
		if seen[skill.Name] {
			return nil, fmt.Errorf("duplicate skill name %q", skill.Name)
		}
		seen[skill.Name] = true
		loaded = append(loaded, skill)
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].Name < loaded[j].Name })
	return loaded, nil
}

func isSkillObject(prefix, key string) bool {
	relative := key
	if prefix != "" {
		if !strings.HasPrefix(key, prefix) {
			return false
		}
		relative = strings.TrimPrefix(key, prefix)
	}
	return strings.Count(relative, "/") == 1 && strings.HasSuffix(relative, "/SKILL.md")
}

func readSkillObject(ctx context.Context, key string, read func(context.Context, string) (io.ReadCloser, error)) ([]byte, error) {
	object, err := read(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("skill %q: read SKILL.md: %w", key, err)
	}
	defer object.Close()
	data, err := io.ReadAll(io.LimitReader(object, maxSkillFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("skill %q: read SKILL.md: %w", key, err)
	}
	if len(data) > maxSkillFileSize {
		return nil, fmt.Errorf("skill %q: SKILL.md exceeds %d bytes", key, maxSkillFileSize)
	}
	return data, nil
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
