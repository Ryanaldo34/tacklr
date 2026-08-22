package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	abstractions "github.com/microsoft/kiota-abstractions-go"
	khttp "github.com/microsoft/kiota-http-go"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	msgraphgocore "github.com/microsoftgraph/msgraph-sdk-go-core"
	"github.com/microsoftgraph/msgraph-sdk-go/drives"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
)

const graphAPIRoot = "https://graph.microsoft.com/v1.0"

// graphSDK is the official msgraph-sdk-go client used by graphProvider.
type graphSDK struct {
	client *msgraphsdk.GraphServiceClient
}

// graphTokenAuth sends the live TokenHolder bearer token on each Graph request.
type graphTokenAuth struct {
	holder *TokenHolder
}

func (a *graphTokenAuth) AuthenticateRequest(ctx context.Context, request *abstractions.RequestInformation, _ map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tok := ""
	if a.holder != nil {
		tok = a.holder.Current().Token
	}
	if tok == "" {
		return ErrAuthExpired
	}
	request.Headers.Add("Authorization", "Bearer "+tok)
	return nil
}

func newGraphSDK(holder *TokenHolder, base string, httpClient *http.Client) (*graphSDK, error) {
	opts := msgraphsdk.GetDefaultClientOptions()
	var parent http.RoundTripper
	if httpClient != nil {
		parent = httpClient.Transport
	}
	// Official middleware (URL rewrite /me, compression, redirects). Tests
	// inject testhttp.Server.Client so only the parent transport is replaced.
	client := &http.Client{
		Transport: khttp.NewCustomTransportWithParentTransport(parent, msgraphgocore.GetDefaultMiddlewaresWithOptions(&opts)...),
		Timeout:   httpClientTimeout(httpClient),
	}
	adapter, err := msgraphsdk.NewGraphRequestAdapterWithParseNodeFactoryAndSerializationWriterFactoryAndHttpClient(
		&graphTokenAuth{holder: holder}, nil, nil, client)
	if err != nil {
		return nil, fmt.Errorf("vfs: msgraph client: %w", err)
	}
	if b := strings.TrimSpace(base); b != "" {
		adapter.SetBaseUrl(strings.TrimRight(b, "/"))
	} else {
		adapter.SetBaseUrl(graphAPIRoot)
	}
	return &graphSDK{client: msgraphsdk.NewGraphServiceClient(adapter)}, nil
}

func httpClientTimeout(httpClient *http.Client) time.Duration {
	if httpClient != nil && httpClient.Timeout > 0 {
		return httpClient.Timeout
	}
	return 100 * time.Second
}

func (g *graphSDK) adapter() abstractions.RequestAdapter {
	return g.client.GetAdapter()
}

func (g *graphSDK) absURL(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	return strings.TrimRight(g.adapter().GetBaseUrl(), "/") + path
}

func (g *graphSDK) item(driveID, itemID string) *drives.ItemItemsDriveItemItemRequestBuilder {
	return g.client.Drives().ByDriveId(driveID).Items().ByDriveItemId(itemID)
}

func (g *graphSDK) resolveRoot(ctx context.Context, driveID, itemID, siteID string) (string, string, error) {
	if driveID == "" {
		var drive models.Driveable
		var err error
		if siteID != "" {
			drive, err = g.client.Sites().BySiteId(siteID).Drive().Get(ctx, nil)
		} else {
			drive, err = g.client.Me().Drive().Get(ctx, nil)
		}
		if err != nil {
			return "", "", mapGraphError(err)
		}
		if drive == nil || drive.GetId() == nil || *drive.GetId() == "" {
			return "", "", fmt.Errorf("vfs: msgraph drive id missing")
		}
		driveID = *drive.GetId()
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

func (g *graphSDK) GetItem(ctx context.Context, driveID, itemID string) (graphItem, error) {
	var item models.DriveItemable
	var err error
	if itemID == "" {
		item, err = g.client.Drives().ByDriveId(driveID).Root().Get(ctx, nil)
	} else {
		item, err = g.item(driveID, itemID).Get(ctx, nil)
	}
	return graphItemOrErr(item, err)
}

func (g *graphSDK) GetByPath(ctx context.Context, driveID, itemID, rel string) (graphItem, error) {
	if rel == "" {
		return g.GetItem(ctx, driveID, itemID)
	}
	// Kiota does not generate ItemWithPath; WithUrl uses the official builder.
	item, err := g.item(driveID, orRootID(itemID)).WithUrl(g.absURL(graphItemURL(driveID, itemID, rel))).Get(ctx, nil)
	return graphItemOrErr(item, err)
}

func (g *graphSDK) ListChildren(ctx context.Context, driveID, itemID string) ([]graphItem, error) {
	page, err := g.item(driveID, itemID).Children().Get(ctx, nil)
	if err != nil {
		return nil, mapGraphError(err)
	}
	if page == nil {
		return nil, nil
	}
	iter, err := msgraphgocore.NewPageIterator[models.DriveItemable](page, g.adapter(), models.CreateDriveItemCollectionResponseFromDiscriminatorValue)
	if err != nil {
		return nil, err
	}
	var out []graphItem
	err = iter.Iterate(ctx, func(item models.DriveItemable) bool {
		out = append(out, graphItemFrom(item))
		return true
	})
	if err != nil {
		return nil, mapGraphError(err)
	}
	return out, nil
}

func (g *graphSDK) GetContent(ctx context.Context, driveID, itemID string) (io.ReadCloser, int64, error) {
	data, err := g.item(driveID, itemID).Content().Get(ctx, nil)
	if err != nil {
		return nil, 0, mapGraphError(err)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (g *graphSDK) PutContent(ctx context.Context, driveID, itemID, name, parentID string, r io.Reader, size int64) (graphItem, error) {
	_ = size
	body, err := io.ReadAll(r)
	if err != nil {
		return graphItem{}, err
	}
	var item models.DriveItemable
	if itemID == "" {
		item, err = g.item(driveID, orRootID(parentID)).Content().
			WithUrl(g.absURL(graphItemURL(driveID, parentID, name)+":/content")).
			Put(ctx, body, nil)
	} else {
		item, err = g.item(driveID, itemID).Content().Put(ctx, body, nil)
	}
	return graphItemOrErr(item, err)
}

func (g *graphSDK) CreateFolder(ctx context.Context, driveID, parentID, name string) (graphItem, error) {
	folder := models.NewDriveItem()
	folder.SetName(&name)
	folder.SetFolder(models.NewFolder())
	folder.SetAdditionalData(map[string]any{"@microsoft.graph.conflictBehavior": "fail"})
	item, err := g.item(driveID, parentID).Children().Post(ctx, folder, nil)
	return graphItemOrErr(item, err)
}

func (g *graphSDK) Delete(ctx context.Context, driveID, itemID string) error {
	return mapGraphError(g.item(driveID, itemID).Delete(ctx, nil))
}

func orRootID(id string) string {
	if id == "" {
		return "root"
	}
	return id
}

func graphItemOrErr(item models.DriveItemable, err error) (graphItem, error) {
	if err != nil {
		return graphItem{}, mapGraphError(err)
	}
	if item == nil {
		return graphItem{}, ErrNotExist
	}
	return graphItemFrom(item), nil
}

func graphItemFrom(item models.DriveItemable) graphItem {
	if item == nil {
		return graphItem{}
	}
	g := graphItem{IsDir: item.GetFolder() != nil}
	if id := item.GetId(); id != nil {
		g.ID = *id
	}
	if name := item.GetName(); name != nil {
		g.Name = *name
	}
	if size := item.GetSize(); size != nil {
		g.Size = *size
	}
	if lm := item.GetLastModifiedDateTime(); lm != nil {
		g.LastModified = lm.UTC().Format(time.RFC3339)
	}
	if parent := item.GetParentReference(); parent != nil {
		if pid := parent.GetId(); pid != nil {
			g.ParentID = *pid
		}
	}
	if file := item.GetFile(); file != nil {
		if mime := file.GetMimeType(); mime != nil {
			g.Mime = *mime
		}
	} else if pkg := item.GetPackageEscaped(); pkg != nil {
		if t := pkg.GetTypeEscaped(); t != nil {
			g.Mime = *t
		}
	}
	return g
}

func mapGraphError(err error) error {
	var sc interface{ GetStatusCode() int }
	if errors.As(err, &sc) {
		switch sc.GetStatusCode() {
		case http.StatusUnauthorized:
			return ErrAuthExpired
		case http.StatusNotFound:
			return ErrNotExist
		case http.StatusForbidden:
			return ErrPermission
		case http.StatusConflict:
			return ErrExist
		}
	}
	return err
}
