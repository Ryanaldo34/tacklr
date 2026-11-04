package session_test

import (
	"bytes"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

func TestAESGCM_opensWhatItSealedOnlyForTheSameSession(t *testing.T) {
	ctx := t.Context()
	s, err := session.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}

	sealed, _ := s.Seal(ctx, []byte("token"), []byte("s1"))
	plain, err := s.Open(ctx, sealed, []byte("s1"))
	_, wrongSession := s.Open(ctx, sealed, []byte("s2"))

	if err != nil || string(plain) != "token" || wrongSession == nil {
		t.Fatalf("open = %q, %v; other session err = %v", plain, err, wrongSession)
	}
}

func TestAESGCM_needsA32ByteKeyAndAWholeCiphertext(t *testing.T) {
	s, _ := session.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	_, short := session.NewAESGCM([]byte("too short"))
	_, truncated := s.Open(t.Context(), []byte("x"), []byte("s"))

	if short == nil || truncated == nil {
		t.Fatalf("short key %v, truncated ciphertext %v; want both refused", short, truncated)
	}
}
