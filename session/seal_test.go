package session_test

import (
	"bytes"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

// TestAESGCM_opensWhatItSealedOnlyForTheSameSessionAndAWholeCiphertext: a
// sealer with a 32-byte key opens what it sealed for the same session. It
// refuses a short key, the ciphertext of another session and a truncated
// ciphertext.
func TestAESGCM_opensWhatItSealedOnlyForTheSameSessionAndAWholeCiphertext(t *testing.T) {
	// Arrange
	ctx := t.Context()
	s, err := session.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}

	// Act
	_, short := session.NewAESGCM([]byte("too short"))
	sealed, _ := s.Seal(ctx, []byte("token"), []byte("s1"))
	plain, err := s.Open(ctx, sealed, []byte("s1"))
	_, wrongSession := s.Open(ctx, sealed, []byte("s2"))
	_, truncated := s.Open(ctx, []byte("x"), []byte("s1"))

	// Assert
	if err != nil || string(plain) != "token" {
		t.Fatalf("open = %q, %v; want the token", plain, err)
	}
	if short == nil || wrongSession == nil || truncated == nil {
		t.Fatalf("short key %v, other session %v, truncated ciphertext %v; want each refused", short, wrongSession, truncated)
	}
}
