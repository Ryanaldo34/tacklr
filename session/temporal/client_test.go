package temporal

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// nopWorkflowClient is a Temporal client whose workflows are gone: signals
// land, queries find no workflow. err, when set, is every call's failure.
type nopWorkflowClient struct {
	client.Client
	err error
}

func (c nopWorkflowClient) SignalWorkflow(context.Context, string, string, string, any) error {
	return c.err
}

func (c nopWorkflowClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	if c.err != nil {
		return nil, c.err
	}
	return nil, serviceerror.NewNotFound("workflow not found")
}

func open(c client.Client, secrets session.SecretStorage) (*Client, *session.MemoryStore) {
	store := session.NewMemoryStore()
	return Open(c, Config{Store: store, Secrets: secrets, DisableStreams: true}), store
}

// TestOpen_needsAClientAStoreAndSecrets: Open panics without any of them.
func TestOpen_needsAClientAStoreAndSecrets(t *testing.T) {
	store, secrets := session.NewMemoryStore(), session.NewMemorySecretStorage()
	for name, cfg := range map[string]struct {
		c   client.Client
		cfg Config
	}{
		"no client":  {nil, Config{Store: store, Secrets: secrets}},
		"no store":   {nopWorkflowClient{}, Config{Secrets: secrets}},
		"no secrets": {nopWorkflowClient{}, Config{Store: store}},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("want a panic")
				}
			}()
			Open(cfg.c, cfg.cfg)
		})
	}
}

// TestClient_aMissingWorkflowIsNotFoundAndOtherErrorsStay: Temporal's
// not-found is session.ErrNotFound; any other failure (an unreachable
// server) is returned as it is, never hidden as not-found.
func TestClient_aMissingWorkflowIsNotFoundAndOtherErrorsStay(t *testing.T) {
	down := serviceerror.NewUnavailable("frontend down")
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"a missing workflow", serviceerror.NewNotFound("workflow not found"), session.ErrNotFound},
		{"an unreachable server", down, down},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt, _ := open(nopWorkflowClient{err: tc.err}, session.NewMemorySecretStorage())
			_, statusErr := rt.Status(t.Context(), "s")
			_, jobsErr := rt.Jobs(t.Context(), "s")
			for name, err := range map[string]error{
				"prompt": rt.Prompt(t.Context(), "s", session.Prompt{Text: "x"}),
				"cancel": rt.Cancel(t.Context(), "s"),
				"status": statusErr,
				"jobs":   jobsErr,
			} {
				if !errors.Is(err, tc.want) {
					t.Errorf("%s = %v, want %v", name, err, tc.want)
				}
			}
		})
	}
}

type failPutSecrets struct{ session.SecretStorage }

var errVaultSealed = errors.New("vault sealed")

func (failPutSecrets) Put(context.Context, session.ID, session.Secrets) error {
	return errVaultSealed
}

// TestClient_aPromptOrResumeFailsWhenTheVaultIsSealed: credentials that
// cannot be stored stop the wake that brought them.
func TestClient_aPromptOrResumeFailsWhenTheVaultIsSealed(t *testing.T) {
	rt, _ := open(nopWorkflowClient{}, failPutSecrets{})
	auth := session.AuthContext{Bindings: []mount.Binding{{Provider: "gdrive", Auth: mount.Credential{Token: "tok"}}}}
	for name, err := range map[string]error{
		"prompt": rt.Prompt(t.Context(), "s", session.Prompt{Text: "x", Auth: auth}),
		"resume": rt.Resume(t.Context(), "s", session.Resume{Auth: auth}),
	} {
		if !errors.Is(err, errVaultSealed) {
			t.Errorf("%s = %v, want %v", name, err, errVaultSealed)
		}
	}
}

// TestClient_aClosedSessionIsGoneWithItsData: Close deletes the session's
// secrets and its record; after it, the session has no status, no jobs and
// takes no prompt, and its events still replay from the fallback log.
func TestClient_aClosedSessionIsGoneWithItsData(t *testing.T) {
	secrets := session.NewMemorySecretStorage()
	if err := secrets.Put(t.Context(), "s", session.Secrets{Auth: session.AuthContext{Bindings: []mount.Binding{{
		Provider: "gdrive", Auth: mount.Credential{Token: "tok"},
	}}}}); err != nil {
		t.Fatal(err)
	}
	rt, store := open(nopWorkflowClient{}, secrets)
	if _, err := store.Save(t.Context(), "s", nil, ""); err != nil {
		t.Fatal(err)
	}

	closeErr := rt.Close(t.Context(), "s")
	got, secretsErr := secrets.Get(t.Context(), "s")
	_, _, loadErr := store.Load(t.Context(), "s")
	_, statusErr := rt.Status(t.Context(), "s")
	_, jobsErr := rt.Jobs(t.Context(), "s")
	promptErr := rt.Prompt(t.Context(), "s", session.Prompt{Text: "again"})
	head, headErr := rt.Head(t.Context(), "s")
	sub, subErr := rt.Subscribe(t.Context(), "s", 0)

	if closeErr != nil || secretsErr != nil || len(got.Auth.Bindings) != 0 || !errors.Is(loadErr, session.ErrNotFound) {
		t.Fatalf("close %v, secrets %+v %v, record %v; want the secrets and the record gone", closeErr, got, secretsErr, loadErr)
	}
	for name, err := range map[string]error{"status": statusErr, "jobs": jobsErr, "prompt": promptErr} {
		if !errors.Is(err, session.ErrNotFound) {
			t.Errorf("%s after close = %v, want ErrNotFound", name, err)
		}
	}
	if head != 0 || headErr != nil || subErr != nil {
		t.Fatalf("head %d %v, subscribe %v; want the fallback log's", head, headErr, subErr)
	}
	_ = sub.Close()
}
