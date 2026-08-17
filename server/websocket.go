package server

import "github.com/coder/websocket/wsjson"

// wsWriteJSON is the WebSocket JSON write implementation. Tests may swap it.
var wsWriteJSON = wsjson.Write
