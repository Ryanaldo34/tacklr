package durableobject

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
)

// Client implements session.Client over a Platform: each call goes to the
// session's object. Credentials travel to the object, which seals them;
// state is checked here, so a bad value fails the caller.
type Client struct {
	platform Platform
}

// NewClient returns a Client over p. It panics without p.
func NewClient(p Platform) *Client {
	if p == nil {
		panic("durableobject: NewClient requires a Platform")
	}
	return &Client{platform: p}
}

// CreateSession implements session.Client.
func (c *Client) CreateSession(ctx context.Context, req session.CreateSession) (session.ID, error) {
	if req.Worker != "" && req.Specialist != "" {
		return "", fmt.Errorf("%w: specialist and worker are exclusive", session.ErrInvalid)
	}
	id := req.SessionID
	if id == "" {
		id = session.ID(uuid.NewString())
	}
	state, err := adapter.EncodeState(req.State)
	if err != nil {
		return "", err
	}
	err = c.platform.Object(id).Create(ctx, Init{
		SessionID:  id,
		Specialist: req.Specialist,
		Worker:     req.Worker,
		Parent:     req.Parent,
		Mounts:     req.Mounts,
		MCPServers: session.DurableMCPServers(req.MCPServers),
		State:      state,
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

// Prompt implements session.Client.
func (c *Client) Prompt(ctx context.Context, id session.ID, msg session.Prompt) error {
	state, err := adapter.EncodeState(msg.State)
	if err != nil {
		return err
	}
	msg.State, msg.MCPServers = state, session.DurableMCPServers(msg.MCPServers)
	return c.platform.Object(id).Wake(ctx, session.Wake{Kind: session.WakePrompt, Prompt: msg})
}

// Resume implements session.Client.
func (c *Client) Resume(ctx context.Context, id session.ID, resume session.Resume) error {
	state, err := adapter.EncodeState(resume.State)
	if err != nil {
		return err
	}
	resume.State = state
	return c.platform.Object(id).Wake(ctx, session.Wake{Kind: session.WakeResume, Resume: resume})
}

// Cancel implements session.Client.
func (c *Client) Cancel(ctx context.Context, id session.ID) error {
	return c.platform.Object(id).Wake(ctx, session.Wake{Kind: session.WakeCancel})
}

// Close implements session.Client: the session's object closes its jobs and
// deletes what it saved. Closing a closed session is not an error.
func (c *Client) Close(ctx context.Context, id session.ID) error {
	return c.platform.Object(id).Wake(ctx, session.Wake{Kind: session.WakeClose})
}

// Status implements session.Client.
func (c *Client) Status(ctx context.Context, id session.ID) (session.Status, error) {
	st, err := c.platform.Object(id).Status(ctx)
	if err != nil {
		return session.Status{ID: id, State: session.Unknown}, err
	}
	return st, nil
}

// Head implements session.Client.
func (c *Client) Head(ctx context.Context, id session.ID) (session.Seq, error) {
	return c.platform.Object(id).Head(ctx)
}

// Subscribe implements session.Client.
func (c *Client) Subscribe(ctx context.Context, id session.ID, after session.Seq) (session.Subscription, error) {
	return c.platform.Object(id).Subscribe(ctx, after)
}

// Jobs implements session.Client: the status of each job the session runs,
// in start order.
func (c *Client) Jobs(ctx context.Context, parent session.ID) ([]session.Status, error) {
	ids, err := c.platform.Object(parent).Jobs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]session.Status, 0, len(ids))
	var errs []error
	for _, id := range ids {
		st, err := c.platform.Object(id).Status(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out = append(out, st)
	}
	return out, errors.Join(errs...)
}

var _ session.Client = (*Client)(nil)
