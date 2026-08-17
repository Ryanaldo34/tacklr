package server

import "testing"

func TestACPTransportRegistry_knownMethods(t *testing.T) {
	cases := []struct {
		method string
		want   acpTransportFlags
	}{
		{acpMethodInitialize, acpTransportFlags{connLevelResult: true}},
		{acpMethodAuthenticate, acpTransportFlags{connLevelResult: true}},
		{acpMethodSessionNew, acpTransportFlags{connLevelResult: true, resultSessionConnLevel: true}},
		{acpMethodSessionLoad, acpTransportFlags{connLevelResult: true, resultSessionConnLevel: true, allowsUnattachedSession: true}},
		{acpMethodSessionPrompt, acpTransportFlags{requiresSessionHeader: true}},
		{acpMethodSessionResume, acpTransportFlags{requiresSessionHeader: true}},
		{acpMethodSessionCancel, acpTransportFlags{requiresSessionHeader: true}},
		{acpMethodSessionSetConfigOption, acpTransportFlags{requiresSessionHeader: true}},
		{acpMethodSessionClose, acpTransportFlags{requiresSessionHeader: true}},
		{methodVFSBind, acpTransportFlags{requiresSessionHeader: true}},
		{methodVFSRefresh, acpTransportFlags{requiresSessionHeader: true}},
		{methodVFSUnbind, acpTransportFlags{requiresSessionHeader: true}},
	}
	for _, tc := range cases {
		got := acpTransportFlagsFor(tc.method)
		if got != tc.want {
			t.Errorf("acpTransportFlagsFor(%q) = %+v, want %+v", tc.method, got, tc.want)
		}
	}
}

func TestACPTransportRegistry_unknownMethod(t *testing.T) {
	if got := acpTransportFlagsFor("session/foo"); got != (acpTransportFlags{}) {
		t.Fatalf("unknown method flags = %+v", got)
	}
}
