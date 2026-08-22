package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/contrib/workflowstreams"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Activities are the Inference and Tool bodies registered on the worker.
type Activities struct {
	Catalog        durable.Catalog
	Snapshots      durable.SnapshotStore
	Projection     vfs.Projection
	Fallback       durable.EventLog
	DisableStreams bool
}

// InferenceInput is the typed Inference activity argument.
type InferenceInput struct {
	SessionID     durable.SessionID
	AgentID       string
	MCPServers    []mcp.MCPConfig
	Etag          string
	User          *streaming.Message
	HadToolRound  bool
	ModelRequests int
	Resume        map[string][]byte
	Auth          durable.AuthContext
	Mounts        []durable.MountRecipe
}

// InferenceOutput is the typed Inference activity result.
type InferenceOutput struct {
	Etag      string
	Complete  bool
	ToolCalls []streaming.ToolCall
}

// ToolInput is the typed Tool activity argument.
type ToolInput struct {
	SessionID  durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Etag       string
	Call       streaming.ToolCall
	Auth       durable.AuthContext
	Mounts     []durable.MountRecipe
}

// ToolOutput is the typed Tool activity result.
type ToolOutput struct {
	Etag        string
	Interrupted bool
	InterruptID string
}

func (a *Activities) Inference(ctx context.Context, in InferenceInput) (InferenceOutput, error) {
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if activity.IsActivity(ctx) && activity.GetInfo(ctx).Attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicRetry, streaming.StreamEvent{Type: streaming.StreamEventError, Content: "retry"}, true)
	}
	h, ms, etag, err := a.harness(ctx, in.SessionID, in.AgentID, in.MCPServers, in.Etag, in.Auth, in.Mounts)
	if err != nil {
		return InferenceOutput{}, err
	}
	defer func() {
		h.Close()
		durable.CloseTurnVFS(ms, string(in.SessionID), "inference")
	}()
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	if len(in.Resume) > 0 {
		if err := h.ApplyResume(in.Resume); err != nil {
			return InferenceOutput{}, err
		}
		if pending := h.PendingToolCalls(); len(pending) > 0 {
			etag, err = a.save(ctx, in.SessionID, in.AgentID, h, etag, in.Mounts)
			return InferenceOutput{Etag: etag, ToolCalls: pending}, err
		}
	}
	if in.User != nil {
		if err := h.AbsorbUser(ctx, in.User, out); err != nil {
			return InferenceOutput{}, err
		}
	}
	st := &tacklr.TurnState{HadToolRound: in.HadToolRound, ModelRequests: in.ModelRequests}
	step, err := h.RunInference(ctx, st, out)
	if err != nil {
		return InferenceOutput{}, err
	}
	etag, err = a.save(ctx, in.SessionID, in.AgentID, h, etag, in.Mounts)
	if err != nil {
		return InferenceOutput{}, err
	}
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, "inference")
	}
	return InferenceOutput{Etag: etag, Complete: step.Complete, ToolCalls: step.ToolCalls}, nil
}

func (a *Activities) Tool(ctx context.Context, in ToolInput) (ToolOutput, error) {
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if activity.IsActivity(ctx) && activity.GetInfo(ctx).Attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicRetry, streaming.StreamEvent{Type: streaming.StreamEventError, Content: "retry"}, true)
	}
	h, ms, etag, err := a.harness(ctx, in.SessionID, in.AgentID, in.MCPServers, in.Etag, in.Auth, in.Mounts)
	if err != nil {
		return ToolOutput{}, err
	}
	defer func() {
		h.Close()
		durable.CloseTurnVFS(ms, string(in.SessionID), "tool")
	}()
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	step, runErr := h.RunToolCall(ctx, in.Call, out)
	etag, saveErr := a.save(ctx, in.SessionID, in.AgentID, h, etag, in.Mounts)
	if saveErr != nil {
		if runErr != nil {
			return ToolOutput{}, fmt.Errorf("tool: %w: persist: %w", runErr, saveErr)
		}
		return ToolOutput{}, saveErr
	}
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, in.Call.Name)
	}
	return ToolOutput{Etag: etag, Interrupted: step.Interrupted, InterruptID: step.InterruptID}, nil
}

func (a *Activities) harness(ctx context.Context, id durable.SessionID, agentID string, extraMCP []mcp.MCPConfig, etag string, auth durable.AuthContext, mounts []durable.MountRecipe) (*tacklr.AgentHarness, *vfs.MountSession, string, error) {
	spec, ok := a.Catalog.Lookup(agentID)
	if !ok {
		return nil, nil, "", durable.ErrAgentNotFound
	}
	proj := a.Projection
	if proj == nil {
		proj = vfs.DirectProjection{}
	}
	ms, err := durable.OpenTurnVFS(ctx, string(id), spec, durable.BindingsForTurn(mounts, auth), proj)
	if err != nil {
		return nil, nil, "", err
	}
	opts := spec.Options
	opts.SessionID = string(id)
	opts.Store = nil
	opts.MountSession = ms
	if len(extraMCP) > 0 {
		mcpConfigs := make([]mcp.MCPConfig, 0, len(opts.MCPConfigs)+len(extraMCP))
		mcpConfigs = append(mcpConfigs, opts.MCPConfigs...)
		mcpConfigs = append(mcpConfigs, extraMCP...)
		opts.MCPConfigs = mcpConfigs
	}
	h, err := tacklr.NewAgent(ctx, opts)
	if err != nil {
		durable.CloseTurnVFS(ms, string(id), "construct")
		return nil, nil, "", err
	}
	if etag != "" {
		snap, loaded, loadErr := a.Snapshots.Load(ctx, id)
		if loadErr == nil {
			if err := h.RestoreCheckpoint(snap.Checkpoint); err != nil {
				h.Close()
				durable.CloseTurnVFS(ms, string(id), "restore")
				return nil, nil, "", err
			}
			etag = loaded
		} else if !errors.Is(loadErr, durable.ErrSessionNotFound) {
			h.Close()
			durable.CloseTurnVFS(ms, string(id), "load")
			return nil, nil, "", loadErr
		}
	}
	return h, ms, etag, nil
}

func (a *Activities) save(ctx context.Context, id durable.SessionID, agentID string, h *tacklr.AgentHarness, etag string, mounts []durable.MountRecipe) (string, error) {
	cp, err := h.Checkpoint()
	if err != nil {
		return "", err
	}
	return a.Snapshots.Save(ctx, id, durable.Snapshot{AgentID: agentID, Checkpoint: *cp, Mounts: mounts}, etag)
}

const streamBatchInterval = 200 * time.Millisecond

func (a *Activities) openStream(ctx context.Context) *workflowstreams.Client {
	if a.DisableStreams || !activity.IsActivity(ctx) {
		return nil
	}
	c, err := workflowstreams.NewClientFromActivity(ctx, workflowstreams.Options{
		BatchInterval: streamBatchInterval,
	})
	if err != nil {
		return nil
	}
	return c
}

func closeStream(ctx context.Context, c *workflowstreams.Client) {
	if c != nil {
		_ = c.Close(ctx)
	}
}

func (a *Activities) emitter(ctx context.Context, stream *workflowstreams.Client, sessionID durable.SessionID) func(streaming.StreamEvent) {
	first := true
	return func(ev streaming.StreamEvent) {
		force := first || ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventInterrupt || ev.Type == streaming.StreamEventError
		first = false
		_ = a.publish(ctx, stream, sessionID, durable.TopicEvents, ev, force)
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, ev.Type)
		}
	}
}

func (a *Activities) publish(ctx context.Context, stream *workflowstreams.Client, sessionID durable.SessionID, topic string, ev streaming.StreamEvent, force bool) error {
	var streamErr error
	if stream != nil {
		stream.Topic(topic).Publish(ev, force)
		if force {
			streamErr = stream.Flush(ctx)
		}
	}
	if a.Fallback != nil {
		if err := a.Fallback.Append(ctx, sessionID, topic, ev); err != nil {
			return err
		}
	}
	return streamErr
}
