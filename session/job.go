package session

// ChildResult is a blocking specialist's output for the call that waited
// on it. Failed is a child that failed: Output is then its error.
type JobResult struct {
	Output string `json:"output"`
	Failed bool   `json:"failed,omitempty"`
}
