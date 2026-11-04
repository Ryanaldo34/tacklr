package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/internal/httprange"
	"github.com/ryanaldo34/tacklr/vfs/internal/rest"
)

// graphItemFields is the driveItem $select: only what GraphItem keeps.
const graphItemFields = "id,name,size,cTag,lastModifiedDateTime,parentReference,folder,file,package"

// microsoftGraph implements GraphAPI with the Graph v1.0 REST API.
type microsoftGraph struct {
	c rest.Client
}

// NewMicrosoftGraph returns a GraphAPI that authenticates with holder
// (required). base is the API root, ending in "/" ("" for the public Graph
// endpoint; set it for a national cloud or a proxy); httpClient nil is
// http.DefaultClient. Pass the result to Graph.
func NewMicrosoftGraph(holder *TokenHolder, base string, httpClient *http.Client) GraphAPI {
	return microsoftGraph{c: apiClient(holder, base, "https://graph.microsoft.com/v1.0/", httpClient)}
}

// graphDriveItem is the driveItem JSON GraphItem is read from.
type graphDriveItem struct {
	ID                   string    `json:"id"`
	Name                 string    `json:"name"`
	Size                 int64     `json:"size"`
	CTag                 string    `json:"cTag"`
	LastModifiedDateTime time.Time `json:"lastModifiedDateTime"`
	ParentReference      struct {
		ID string `json:"id"`
	} `json:"parentReference"`
	Folder *struct{} `json:"folder"`
	File   *struct {
		MimeType string `json:"mimeType"`
	} `json:"file"`
	Package *struct {
		Type string `json:"type"`
	} `json:"package"`
}

// graphItem is the item as GraphItem. A package (OneNote) has its type as
// Mime and no file facet.
func (d graphDriveItem) graphItem() GraphItem {
	item := GraphItem{
		ID: d.ID, Name: d.Name, ParentID: d.ParentReference.ID,
		Size: d.Size, IsDir: d.Folder != nil,
		LastModified: d.LastModifiedDateTime.UTC(), CTag: d.CTag,
	}
	switch {
	case d.File != nil:
		item.Mime = d.File.MimeType
	case d.Package != nil:
		item.Mime = d.Package.Type
	}
	return item
}

// ResolveRoot implements GraphAPI.
func (g microsoftGraph) ResolveRoot(ctx context.Context, driveID, itemID, siteID string) (string, string, error) {
	if driveID == "" {
		drivePath := "me/drive"
		if siteID != "" {
			drivePath = "sites/" + url.PathEscape(siteID) + "/drive"
		}
		var drive struct {
			ID string `json:"id"`
		}
		if err := g.c.Call(ctx, http.MethodGet, drivePath, url.Values{"$select": {"id"}}, nil, &drive); err != nil {
			return "", "", mapGraphError(err)
		}
		if drive.ID == "" {
			return "", "", errors.New("vfs: msgraph drive id missing")
		}
		driveID = drive.ID
	}
	item, err := g.GetItem(ctx, driveID, itemID)
	if err != nil {
		return "", "", err
	}
	if !item.IsDir {
		return "", "", errors.New("vfs: msgraph itemId is not a folder")
	}
	return driveID, item.ID, nil
}

// GetItem implements GraphAPI. An empty itemID is the drive root.
func (g microsoftGraph) GetItem(ctx context.Context, driveID, itemID string) (GraphItem, error) {
	return g.GetByPath(ctx, driveID, itemID, "")
}

// GetByPath implements GraphAPI.
func (g microsoftGraph) GetByPath(ctx context.Context, driveID, itemID, rel string) (GraphItem, error) {
	var d graphDriveItem
	err := g.c.Call(ctx, http.MethodGet, graphItemPath(driveID, itemID, rel), url.Values{"$select": {graphItemFields}}, nil, &d)
	return d.graphItem(), mapGraphError(err)
}

// ListChildren implements GraphAPI. It follows @odata.nextLink to the last
// page.
func (g microsoftGraph) ListChildren(ctx context.Context, driveID, itemID string) ([]GraphItem, error) {
	next, q := graphItemPath(driveID, itemID, "")+"/children", url.Values{"$select": {graphItemFields}}
	var out []GraphItem
	for next != "" {
		var page struct {
			Value    []graphDriveItem `json:"value"`
			NextLink string           `json:"@odata.nextLink"`
		}
		if err := g.c.Call(ctx, http.MethodGet, next, q, nil, &page); err != nil {
			return nil, mapGraphError(err)
		}
		for _, d := range page.Value {
			out = append(out, d.graphItem())
		}
		// The next link carries the query.
		next, q = page.NextLink, nil
	}
	return out, nil
}

// GetContent implements GraphAPI. Graph answers with a redirect to a
// pre-authenticated download URL; the client follows it and drops the
// bearer token when the host changes.
func (g microsoftGraph) GetContent(ctx context.Context, driveID, itemID string, off, n int64) (io.ReadCloser, error) {
	h := http.Header{}
	if r := httprange.Header(off, n); r != "" {
		h.Set("Range", r)
	}
	res, err := g.c.Media(ctx, graphItemPath(driveID, itemID, "")+"/content", nil, h) //nolint:bodyclose // httprange.Body returns the body or closes it
	if err != nil {
		return nil, mapGraphError(err)
	}
	return httprange.Body(res, off, n)
}

// PutContent implements GraphAPI with a simple upload (up to 250 MB; vfs
// writes at most MaxReadFileBytes). An empty itemID creates name under
// parentID.
func (g microsoftGraph) PutContent(ctx context.Context, driveID, itemID, name, parentID string, r io.Reader, _ int64) (GraphItem, error) {
	target := graphItemPath(driveID, itemID, "") + "/content"
	if itemID == "" {
		target = graphItemPath(driveID, parentID, name) + ":/content"
	}
	var d graphDriveItem
	err := g.c.Stream(ctx, http.MethodPut, target, nil, "application/octet-stream", r, &d)
	return d.graphItem(), mapGraphError(err)
}

// CreateFolder implements GraphAPI. An existing name fails with ErrExist.
func (g microsoftGraph) CreateFolder(ctx context.Context, driveID, parentID, name string) (GraphItem, error) {
	folder := struct {
		Name     string   `json:"name"`
		Folder   struct{} `json:"folder"`
		Conflict string   `json:"@microsoft.graph.conflictBehavior"`
	}{Name: name, Conflict: "fail"}
	var d graphDriveItem
	err := g.c.Call(ctx, http.MethodPost, graphItemPath(driveID, parentID, "")+"/children", nil, folder, &d)
	return d.graphItem(), mapGraphError(err)
}

// Delete implements GraphAPI (the item goes to the recycle bin).
func (g microsoftGraph) Delete(ctx context.Context, driveID, itemID string) error {
	return mapGraphError(g.c.Call(ctx, http.MethodDelete, graphItemPath(driveID, itemID, ""), nil, nil, nil))
}

// graphItemPath is the driveItem path for itemID ("" is the drive root),
// or for rel under it when rel is not empty.
func graphItemPath(driveID, itemID, rel string) string {
	p := "drives/" + url.PathEscape(driveID) + "/root"
	if itemID != "" {
		p = "drives/" + url.PathEscape(driveID) + "/items/" + url.PathEscape(itemID)
	}
	if rel == "" {
		return p
	}
	parts := strings.Split(rel, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return p + ":/" + strings.Join(parts, "/")
}

// mapGraphError maps a Graph reply error to the vfs errors.
func mapGraphError(err error) error {
	var rerr *rest.Error
	if !errors.As(err, &rerr) {
		return err
	}
	switch rerr.Code {
	case http.StatusUnauthorized:
		return ErrAuthExpired
	case http.StatusNotFound:
		return ErrNotExist
	case http.StatusForbidden:
		return ErrPermission
	case http.StatusConflict:
		return ErrExist
	default:
		return fmt.Errorf("vfs: msgraph %w", rerr)
	}
}
