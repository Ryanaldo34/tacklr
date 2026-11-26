package tacklr

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestNearPressure_countsOnlyARequestNearTheWindow: a request whose bound
// (the last count plus what was added since) is far below the window's
// pressure commits without a count; one at or over it is counted.
func TestNearPressure_countsOnlyARequestNearTheWindow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		bound, size int
		want        bool
	}{
		{name: "far below the window", bound: 100 + 200, size: 8192},
		{name: "near the window", bound: 7000 + 200, size: 8192, want: true},
		{name: "over the window", bound: 9000, size: 8192, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := nearPressure(tc.bound, tc.size)

			if got != tc.want {
				t.Fatalf("nearPressure(%d, %d) = %v, want %v", tc.bound, tc.size, got, tc.want)
			}
		})
	}
}

// TestCutText_cutsOnARuneBoundaryAndSaysHowToSeeTheRest: a cut ends on
// the rune it would split, and its note says how much is kept and to run a
// narrower command for the rest.
func TestCutText_cutsOnARuneBoundaryAndSaysHowToSeeTheRest(t *testing.T) {
	t.Parallel()
	full := strings.Repeat("─", 10)

	got, kept := cutText(full, 7)

	if kept != 6 || !utf8.ValidString(got) || !strings.HasPrefix(got, "──\n\n") || !strings.Contains(got, "kept the first 6 of 30 bytes") || !strings.HasSuffix(got, "Run a narrower command or query to see the rest.]") {
		t.Fatalf("cutText = %q kept %d, want two runes and the note", got, kept)
	}
}
