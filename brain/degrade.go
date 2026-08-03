package brain

// DegradeMode is a closed enum for soft-fail retrieval paths (string values match telemetry labels).
type DegradeMode string

const (
	DegradeNone            DegradeMode = "none"
	DegradeLexicalOnly     DegradeMode = "lexical_only"
	DegradeContainmentOnly DegradeMode = "containment_only"
)

func (m DegradeMode) String() string {
	if m == "" {
		return string(DegradeNone)
	}
	return string(m)
}
