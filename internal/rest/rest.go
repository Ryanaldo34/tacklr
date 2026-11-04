// Package rest calls the JSON REST APIs the vfs adapters use (Google
// Drive, Docs and Sheets; Microsoft Graph) over net/http and encoding/json.
// It links no vendor SDK.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Client calls one API. Base is the API root and ends in "/"; relative
// paths resolve against it.
type Client struct {
	Base  string
	HTTP  *http.Client
	Token func() string
}

// Error is a non-2xx reply: the HTTP status and the API's error message.
// Google and Graph both send {"error": {"message": "..."}}.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("HTTP %d", e.Code)
	}
	return fmt.Sprintf("HTTP %d: %s", e.Code, e.Message)
}

// Call sends in as JSON (nil: no body) and decodes the reply into out (nil:
// the reply is dropped). path is relative to Base, or an absolute URL under
// Base (a page link the API returned).
func (c Client) Call(ctx context.Context, method, path string, q url.Values, in, out any) error {
	var body io.Reader
	if in != nil {
		body = bytes.NewReader(Encode(in))
	}
	return c.Stream(ctx, method, path, q, "application/json", body, out)
}

// Stream sends body as is with contentType and decodes the reply into out
// (nil: the reply is dropped). A *bytes.Reader body sets Content-Length.
func (c Client) Stream(ctx context.Context, method, path string, q url.Values, contentType string, body io.Reader, out any) error {
	res, err := c.send(ctx, method, path, q, body, contentType, nil)
	if err != nil {
		return err
	}
	return decode(res, out)
}

// Media opens the reply body of a GET, with header (a Range) added. The
// caller closes the body.
func (c Client) Media(ctx context.Context, path string, q url.Values, header http.Header) (*http.Response, error) {
	return c.send(ctx, http.MethodGet, path, q, nil, "", header)
}

// send makes the request and returns the reply when it is 2xx, else an
// *Error.
func (c Client) send(ctx context.Context, method, path string, q url.Values, body io.Reader, contentType string, header http.Header) (*http.Response, error) {
	u, err := c.resolve(path)
	if err != nil {
		return nil, err
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token())
	res, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode/100 == 2 {
		return res, nil
	}
	defer res.Body.Close()
	var reply struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	b, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	_ = json.Unmarshal(b, &reply)
	return nil, &Error{Code: res.StatusCode, Message: reply.Error.Message}
}

// resolve is path's URL. An absolute URL must be under Base, so the bearer
// token never goes to another host.
func (c Client) resolve(path string) (string, error) {
	if !strings.Contains(path, "://") {
		return c.Base + strings.TrimPrefix(path, "/"), nil
	}
	if !strings.HasPrefix(path, c.Base) {
		return "", fmt.Errorf("rest: %s is not under %s", path, c.Base)
	}
	return path, nil
}

// Encode is v as JSON. The wire types hold only strings, finite numbers,
// bools and containers of them, which always encode.
func Encode(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func decode(res *http.Response, out any) error {
	defer res.Body.Close()
	if out == nil {
		_, err := io.Copy(io.Discard, res.Body)
		return err
	}
	return json.NewDecoder(res.Body).Decode(out)
}
