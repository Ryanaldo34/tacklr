package email

import (
	"context"
	"strings"
	"testing"
)

type testProvider struct {
	kind ProviderKind
}

func (p *testProvider) Kind() ProviderKind           { return p.kind }
func (*testProvider) Validate(context.Context) error { return nil }
func (*testProvider) ReadInbox(context.Context, ReadInboxRequest) (Inbox, error) {
	return Inbox{}, nil
}
func (*testProvider) SendEmail(context.Context, SendEmailRequest) (SentEmail, error) {
	return SentEmail{}, nil
}

func TestValidateProvider_supportedKindsAndNilValues(t *testing.T) {
	var typedNil *testProvider
	cases := []struct {
		name     string
		provider Provider
		wantErr  string
	}{
		{name: "disabled"},
		{name: "gmail", provider: &testProvider{kind: ProviderGmail}},
		{name: "outlook", provider: &testProvider{kind: ProviderOutlook}},
		{name: "typed nil", provider: typedNil, wantErr: "must not be nil"},
		{name: "unsupported", provider: &testProvider{kind: "imap"}, wantErr: "unsupported provider"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateProvider(tc.provider)
			if tc.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestSendEmailRequest_validateRequiredContent(t *testing.T) {
	cases := []struct {
		name string
		req  SendEmailRequest
		want string
	}{
		{name: "valid", req: SendEmailRequest{To: []string{"a@example.com"}, Subject: "Subject", Body: "Body"}},
		{name: "missing to", req: SendEmailRequest{Subject: "Subject", Body: "Body"}, want: "recipient"},
		{name: "empty recipient", req: SendEmailRequest{To: []string{"a@example.com"}, CC: []string{" "}, Subject: "Subject", Body: "Body"}, want: "must not be empty"},
		{name: "missing subject", req: SendEmailRequest{To: []string{"a@example.com"}, Body: "Body"}, want: "subject"},
		{name: "missing body", req: SendEmailRequest{To: []string{"a@example.com"}, Subject: "Subject"}, want: "body"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if tc.want == "" && err != nil {
				t.Fatal(err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}
