package tacklr

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs"
)

// AttachDocument places document bytes in the session's ephemeral virtual
// filesystem and adds a compact reference to the conversation context.
func (a *AgentHarness) AttachDocument(ctx context.Context, name string, data []byte) (string, error) {
	if a == nil || a.session == nil {
		return "", fmt.Errorf("attach document: agent session required")
	}
	name = path.Base(strings.TrimSpace(name))
	if name == "." || name == "/" || name == "" || name == ".." {
		return "", fmt.Errorf("attach document: valid file name required")
	}
	if err := a.ensureAttachmentMount(ctx); err != nil {
		return "", err
	}
	a.ensureAttachmentTools()
	virtualPath := "/context/" + name
	if err := a.session.VFS.WriteFile(ctx, virtualPath, data); err != nil {
		return "", fmt.Errorf("attach document %s: %w", name, err)
	}
	a.context.Add(&Message{Role: RoleUser, Content: fmt.Sprintf("Attached document %q is available at %s. Use the virtual filesystem tools to inspect or search it.", name, virtualPath)})
	return virtualPath, nil
}

func (a *AgentHarness) ensureAttachmentTools() {
	known := make(map[string]struct{}, len(a.tools))
	for _, tool := range a.tools {
		known[tool.Name] = struct{}{}
	}
	for _, tool := range newVFSTools(a.session.VFS) {
		if _, ok := known[tool.Name]; !ok {
			a.tools = append(a.tools, tool)
		}
	}
	if a.vfsBridge == nil {
		a.vfsBridge = a.initVFSIndexBridge()
	}
	for _, tool := range newVFSIndexTools(a.vfsBridge) {
		if _, ok := known[tool.Name]; !ok {
			a.tools = append(a.tools, tool)
		}
	}
}

func (a *AgentHarness) ensureAttachmentMount(ctx context.Context) error {
	if a.session.VFS == nil {
		if a.fsRegistry == nil {
			a.fsRegistry = vfs.NewBackendRegistry()
		}
		a.session.VFS = vfs.NewMountSession(a.sessionId, a.fsRegistry)
	}
	if _, _, err := a.session.VFS.Lookup("/context"); err == nil {
		return nil
	}
	if a.fsRegistry == nil {
		return fmt.Errorf("attach document: VFS registry required")
	}
	if a.attachmentFS == nil {
		a.attachmentFS = &vfs.MemoryFactory{ID: "attachments"}
		if err := a.fsRegistry.Register(a.attachmentFS); err != nil {
			return fmt.Errorf("attach document: register memory backend: %w", err)
		}
	}
	if err := a.session.VFS.Mount(ctx, vfs.MountSpec{Point: "/context", Profile: "attachments"}); err != nil {
		return fmt.Errorf("attach document: mount context: %w", err)
	}
	return nil
}
