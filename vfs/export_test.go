package vfs

// MustNewMountSession is a test helper. Production callers use NewMountSession.
func MustNewMountSession(sessionID string, reg *BackendRegistry) *MountSession {
	ms, err := NewMountSession(sessionID, reg)
	if err != nil {
		panic(err)
	}
	return ms
}
