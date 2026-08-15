package brain

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/ryanaldo34/tacklr/vfs"
)

const engramContentType = "text/markdown"

// reservedFrontMatter keys are consumed by the codec (not copied into Properties).
// domain and kind are aliases for Object.Kind.
var reservedFrontMatter = map[string]struct{}{
	"id": {}, "domain": {}, "kind": {}, "slug": {}, "title": {},
}

// EngramFile is the Markdown + YAML front-matter view of a first-class object.
//
// Front-matter reserved keys: id, domain/kind, slug, title. Remaining keys become
// Object.Properties. The body becomes Object.Content.
//
// Parse splits on the first pair of --- fences. A --- line inside the YAML block
// ends front matter (standard). The body after the closing fence may contain ---;
// there is no support for a --- document start inside the YAML mapping itself.
type EngramFile struct {
	ID         uuid.UUID
	Kind       string
	Slug       string
	Title      string
	Properties map[string]any
	Body       string
}

// ParseEngram decodes Markdown with optional YAML front matter.
func ParseEngram(data []byte) (EngramFile, error) {
	s := strings.TrimPrefix(string(data), "\ufeff")
	fm, body, err := splitFrontMatter(s)
	if err != nil {
		return EngramFile{}, err
	}
	return engramFromMap(fm, body)
}

// FormatEngram encodes an Engram as Markdown + YAML front matter.
// Key order is stable: id, domain, slug, title, then remaining property keys sorted.
// vfs_path is never written (Provider-internal).
func FormatEngram(f EngramFile) ([]byte, error) {
	var node yaml.Node
	node.Kind = yaml.MappingNode
	addStr := func(k, v string) {
		if v == "" {
			return
		}
		var kn, vn yaml.Node
		kn.SetString(k)
		vn.SetString(v)
		node.Content = append(node.Content, &kn, &vn)
	}
	add := func(k string, v any) error {
		if v == nil {
			return nil
		}
		if s, ok := v.(string); ok {
			addStr(k, s)
			return nil
		}
		var kn, vn yaml.Node
		kn.SetString(k)
		if err := vn.Encode(v); err != nil {
			return fmt.Errorf("brain: encode front matter %q: %w", k, err)
		}
		node.Content = append(node.Content, &kn, &vn)
		return nil
	}
	if f.ID != uuid.Nil {
		addStr("id", f.ID.String())
	}
	addStr("domain", strings.TrimSpace(f.Kind))
	addStr("slug", strings.TrimSpace(f.Slug))
	addStr("title", strings.TrimSpace(f.Title))
	for _, k := range slices.Sorted(maps.Keys(f.Properties)) {
		if _, reserved := reservedFrontMatter[k]; reserved || k == PropVFSPath {
			continue
		}
		if f.Properties[k] == nil {
			continue
		}
		if err := add(k, f.Properties[k]); err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	buf.Grow(64 + len(f.Body))
	buf.WriteString("---\n")
	if len(node.Content) > 0 {
		enc := yaml.NewEncoder(&buf)
		enc.SetIndent(2)
		// Node was built with SetString/Encode; encoder cannot fail on it.
		_ = enc.Encode(&node)
		_ = enc.Close()
	}
	buf.WriteString("---\n")
	if body := f.Body; body != "" {
		buf.WriteByte('\n')
		buf.WriteString(body)
		if !strings.HasSuffix(body, "\n") {
			buf.WriteByte('\n')
		}
	}
	return buf.Bytes(), nil
}

// ObjectFromEngram maps a parsed file to an Object (no namespace / vfs_path).
func ObjectFromEngram(f EngramFile) Object {
	props := maps.Clone(f.Properties)
	if props == nil {
		props = map[string]any{}
	}
	if s := strings.TrimSpace(f.Slug); s != "" {
		props[PropSlug] = s
	}
	title := strings.TrimSpace(f.Title)
	if title == "" {
		title = strings.TrimSpace(f.Slug)
	}
	return Object{
		ID:          f.ID,
		Kind:        strings.TrimSpace(f.Kind),
		Title:       title,
		Properties:  props,
		Content:     f.Body,
		ContentType: engramContentType,
	}
}

// EngramFromObject serializes a stored object (drops vfs_path from front matter).
func EngramFromObject(obj Object) EngramFile {
	props := maps.Clone(obj.Properties)
	if props == nil {
		props = map[string]any{}
	}
	slug, _ := props[PropSlug].(string)
	slug = strings.TrimSpace(slug)
	delete(props, PropSlug)
	delete(props, PropVFSPath)
	title := strings.TrimSpace(obj.Title)
	if slug == "" {
		slug = Slugify(title)
	}
	if title == "" {
		title = slug
	}
	return EngramFile{
		ID:         obj.ID,
		Kind:       obj.Kind,
		Slug:       slug,
		Title:      title,
		Properties: props,
		Body:       obj.Content,
	}
}

// Slugify is the path slug for an Engram title (same rules as vfs.Slugify).
func Slugify(title string) string {
	return vfs.Slugify(title)
}

// KindSlug is the v1 directory name for a kind (lowercased).
func KindSlug(kind string) string {
	return strings.ToLower(strings.TrimSpace(kind))
}

// IsParentKind reports whether a kind is listed as files (not parts/chunks).
func IsParentKind(spec KindSpec) bool {
	return spec.IsParent || !spec.IsPart
}

func splitFrontMatter(s string) (map[string]any, string, error) {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	if !strings.HasPrefix(s, "---\n") {
		return nil, s, nil
	}
	rest := strings.TrimPrefix(s, "---\n")
	yamlPart, body, ok := strings.Cut(rest, "\n---\n")
	if !ok {
		switch {
		case rest == "---":
			yamlPart, body = "", ""
		case strings.HasPrefix(rest, "---\n"):
			yamlPart, body = "", rest[len("---\n"):]
		case strings.HasSuffix(rest, "\n---"):
			yamlPart, body = strings.TrimSuffix(rest, "\n---"), ""
		default:
			return nil, "", fmt.Errorf("brain: engram front matter: missing closing ---")
		}
	}
	// Conventional blank line after the closing fence is not part of the body.
	body = strings.TrimPrefix(body, "\n")
	fm := map[string]any{}
	if strings.TrimSpace(yamlPart) != "" {
		if err := yaml.Unmarshal([]byte(yamlPart), &fm); err != nil {
			return nil, "", fmt.Errorf("brain: engram front matter: %w", err)
		}
	}
	return fm, body, nil
}

func engramFromMap(fm map[string]any, body string) (EngramFile, error) {
	f := EngramFile{Properties: map[string]any{}, Body: body}
	if fm == nil {
		return f, nil
	}
	for _, k := range slices.Sorted(maps.Keys(fm)) {
		v := fm[k]
		switch k {
		case "id":
			s, ok := yamlString(v)
			if !ok || s == "" {
				return EngramFile{}, fmt.Errorf("brain: engram id must be a UUID string")
			}
			id, err := uuid.Parse(s)
			if err != nil {
				return EngramFile{}, fmt.Errorf("brain: engram id: %w", err)
			}
			f.ID = id
		case "domain", "kind":
			s, ok := yamlString(v)
			if !ok {
				return EngramFile{}, fmt.Errorf("brain: engram %s must be a string", k)
			}
			if f.Kind == "" {
				f.Kind = s
			}
		case "slug":
			s, ok := yamlString(v)
			if !ok {
				return EngramFile{}, fmt.Errorf("brain: engram slug must be a string")
			}
			f.Slug = s
		case "title":
			s, ok := yamlString(v)
			if !ok {
				return EngramFile{}, fmt.Errorf("brain: engram title must be a string")
			}
			f.Title = s
		default:
			f.Properties[k] = v
		}
	}
	return f, nil
}

func yamlString(v any) (string, bool) {
	if v == nil {
		return "", true
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(s), true
}
