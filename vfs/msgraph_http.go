package vfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

const graphAPIRoot = "https://graph.microsoft.com/v1.0"

type graphHTTP struct {
	base   string
	client *http.Client
	holder *TokenHolder
}

func newGraphHTTP(holder *TokenHolder, base string) *graphHTTP {
	if strings.TrimSpace(base) == "" {
		base = graphAPIRoot
	}
	return &graphHTTP{base: strings.TrimRight(base, "/"), client: http.DefaultClient, holder: holder}
}

type graphItemJSON struct {
	ID                   string `json:"id"`
	Name                 string `json:"name"`
	Size                 int64  `json:"size"`
	LastModifiedDateTime string `json:"lastModifiedDateTime"`
	File                 *struct {
		MimeType string `json:"mimeType"`
	} `json:"file"`
	Folder *struct {
		ChildCount int `json:"childCount"`
	} `json:"folder"`
	ParentReference *struct {
		ID      string `json:"id"`
		DriveID string `json:"driveId"`
	} `json:"parentReference"`
	Package *struct {
		Type string `json:"type"`
	} `json:"package"`
}

func (j graphItemJSON) item() GraphItem {
	g := GraphItem{
		ID: j.ID, Name: j.Name, Size: j.Size,
		LastModified: j.LastModifiedDateTime, IsDir: j.Folder != nil,
	}
	if j.ParentReference != nil {
		g.ParentID = j.ParentReference.ID
	}
	if j.File != nil {
		g.Mime = j.File.MimeType
	} else if j.Package != nil {
		g.Mime = j.Package.Type
	}
	return g
}

func (g *graphHTTP) resolveRoot(ctx context.Context, driveID, itemID, siteID string) (string, string, error) {
	if driveID == "" {
		path := "/me/drive"
		if siteID != "" {
			path = "/sites/" + url.PathEscape(siteID) + "/drive"
		}
		var d graphItemJSON
		if err := g.doJSON(ctx, http.MethodGet, path, nil, &d); err != nil {
			return "", "", err
		}
		driveID = d.ID
	}
	item, err := g.GetItem(ctx, driveID, itemID)
	if err != nil {
		return "", "", err
	}
	if !item.IsDir {
		return "", "", fmt.Errorf("vfs: msgraph itemId is not a folder")
	}
	return driveID, item.ID, nil
}

func graphItemURL(driveID, itemID, rel string) string {
	d := "/drives/" + url.PathEscape(driveID)
	switch {
	case itemID == "" && rel == "":
		return d + "/root"
	case itemID == "":
		return d + "/root:/" + encodeGraphRel(rel)
	case rel == "":
		return d + "/items/" + url.PathEscape(itemID)
	default:
		return d + "/items/" + url.PathEscape(itemID) + ":/" + encodeGraphRel(rel)
	}
}

func encodeGraphRel(rel string) string {
	if !strings.Contains(rel, "/") {
		return url.PathEscape(rel)
	}
	parts := strings.Split(rel, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func (g *graphHTTP) getJSONItem(ctx context.Context, path string) (GraphItem, error) {
	var raw graphItemJSON
	if err := g.doJSON(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return GraphItem{}, err
	}
	return raw.item(), nil
}

func (g *graphHTTP) GetItem(ctx context.Context, driveID, itemID string) (GraphItem, error) {
	return g.getJSONItem(ctx, graphItemURL(driveID, itemID, ""))
}

func (g *graphHTTP) GetByPath(ctx context.Context, driveID, itemID, rel string) (GraphItem, error) {
	return g.getJSONItem(ctx, graphItemURL(driveID, itemID, rel))
}

func (g *graphHTTP) ListChildren(ctx context.Context, driveID, itemID string) ([]GraphItem, error) {
	path := graphItemURL(driveID, itemID, "") + "/children"
	var out []GraphItem
	for path != "" {
		var page struct {
			Value    []graphItemJSON `json:"value"`
			NextLink string          `json:"@odata.nextLink"`
		}
		if err := g.doJSON(ctx, http.MethodGet, path, nil, &page); err != nil {
			return nil, err
		}
		out = slices.Grow(out, len(page.Value))
		for _, v := range page.Value {
			out = append(out, v.item())
		}
		path = g.relPath(page.NextLink)
	}
	return out, nil
}

func (g *graphHTTP) GetContent(ctx context.Context, driveID, itemID string) (io.ReadCloser, int64, error) {
	path := graphItemURL(driveID, itemID, "") + "/content"
	req, err := g.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, 0, err
	}
	res, err := g.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if err := graphStatus(res); err != nil {
		_ = res.Body.Close()
		return nil, 0, err
	}
	return res.Body, res.ContentLength, nil
}

func (g *graphHTTP) PutContent(ctx context.Context, driveID, itemID, name, parentID string, r io.Reader, size int64) (GraphItem, error) {
	path := graphItemURL(driveID, itemID, "") + "/content"
	if itemID == "" {
		path = graphItemURL(driveID, parentID, name) + ":/content"
	}
	req, err := g.newRequest(ctx, http.MethodPut, path, r)
	if err != nil {
		return GraphItem{}, err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	res, err := g.client.Do(req)
	if err != nil {
		return GraphItem{}, err
	}
	defer res.Body.Close()
	if err := graphStatus(res); err != nil {
		return GraphItem{}, err
	}
	var raw graphItemJSON
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		return GraphItem{}, err
	}
	return raw.item(), nil
}

func (g *graphHTTP) CreateFolder(ctx context.Context, driveID, parentID, name string) (GraphItem, error) {
	body := struct {
		Name     string   `json:"name"`
		Folder   struct{} `json:"folder"`
		Conflict string   `json:"@microsoft.graph.conflictBehavior"`
	}{Name: name, Conflict: "fail"}
	path := graphItemURL(driveID, parentID, "") + "/children"
	var raw graphItemJSON
	if err := g.doJSON(ctx, http.MethodPost, path, body, &raw); err != nil {
		return GraphItem{}, err
	}
	return raw.item(), nil
}

func (g *graphHTTP) Delete(ctx context.Context, driveID, itemID string) error {
	return g.doJSON(ctx, http.MethodDelete, graphItemURL(driveID, itemID, ""), nil, nil)
}

func (g *graphHTTP) doJSON(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(raw)
	}
	req, err := g.newRequest(ctx, method, path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if err := graphStatus(res); err != nil {
		return err
	}
	if out == nil || res.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (g *graphHTTP) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u := path
	if !strings.HasPrefix(path, "http://") && !strings.HasPrefix(path, "https://") {
		u = g.base + path
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if g.holder != nil {
		tok := g.holder.Current().Token
		if tok == "" {
			return nil, ErrAuthExpired
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	return req, nil
}

func (g *graphHTTP) relPath(next string) string {
	if next == "" {
		return ""
	}
	if strings.HasPrefix(next, g.base) {
		return strings.TrimPrefix(next, g.base)
	}
	return next
}

func graphStatus(res *http.Response) error {
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		return nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 8<<10))
	switch res.StatusCode {
	case http.StatusUnauthorized:
		return ErrAuthExpired
	case http.StatusNotFound:
		return ErrNotExist
	case http.StatusForbidden:
		return ErrPermission
	case http.StatusConflict:
		return ErrExist
	default:
		return fmt.Errorf("vfs: graph http %d", res.StatusCode)
	}
}
