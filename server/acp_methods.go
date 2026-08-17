package server

// acpTransportFlags describe Streamable HTTP routing for one inbound JSON-RPC method.
// Register every method that participates in POST header checks or SSE result delivery here.
type acpTransportFlags struct {
	// requiresSessionHeader: non-notification POST must carry Acp-Session-Id.
	requiresSessionHeader bool
	// connLevelResult: the JSON-RPC result is delivered on the connection-scoped SSE stream.
	connLevelResult bool
	// resultSessionConnLevel: when the result body contains sessionId, force conn-level delivery.
	resultSessionConnLevel bool
	// allowsUnattachedSession: session id need not already be noted on this connection.
	allowsUnattachedSession bool
}

// ACP JSON-RPC method names (client → agent).
const (
	acpMethodInitialize             = "initialize"
	acpMethodAuthenticate           = "authenticate"
	acpMethodLogout                 = "logout"
	acpMethodSessionNew             = "session/new"
	acpMethodSessionLoad            = "session/load"
	acpMethodSessionPrompt          = "session/prompt"
	acpMethodSessionResume          = "session/resume"
	acpMethodSessionCancel          = "session/cancel"
	acpMethodSessionSetConfigOption = "session/set_config_option"
	acpMethodSessionClose           = "session/close"

	methodVFSBind    = "_tacklr/vfs/bind"
	methodVFSRefresh = "_tacklr/vfs/refresh"
	methodVFSUnbind  = "_tacklr/vfs/unbind"
	methodVFSToken   = "_tacklr/vfs/token" // server → client notification
)

var acpTransportRegistry = map[string]acpTransportFlags{
	acpMethodInitialize:             {connLevelResult: true},
	acpMethodAuthenticate:           {connLevelResult: true},
	acpMethodSessionNew:             {connLevelResult: true, resultSessionConnLevel: true},
	acpMethodSessionLoad:            {connLevelResult: true, resultSessionConnLevel: true, allowsUnattachedSession: true},
	acpMethodSessionPrompt:          {requiresSessionHeader: true},
	acpMethodSessionResume:          {requiresSessionHeader: true},
	acpMethodSessionCancel:          {requiresSessionHeader: true},
	acpMethodSessionSetConfigOption: {requiresSessionHeader: true},
	acpMethodSessionClose:           {requiresSessionHeader: true},
	methodVFSBind:                   {requiresSessionHeader: true},
	methodVFSRefresh:                {requiresSessionHeader: true},
	methodVFSUnbind:                 {requiresSessionHeader: true},
}

func acpTransportFlagsFor(method string) acpTransportFlags {
	if flags, ok := acpTransportRegistry[method]; ok {
		return flags
	}
	return acpTransportFlags{}
}
