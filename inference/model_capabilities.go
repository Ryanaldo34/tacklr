package inference

import (
	"strings"

	"github.com/ryanaldo34/tacklr/streaming"
)

// Model media capability is prefix match against the selected model id.
// Unknown models accept text only (safe default for ACP ads and prompt reject).

// Prefixes that imply vision / image input on OpenAI-compatible stacks.
var visionModelPrefixes = []string{
	"gpt-4o",
	"gpt-4.1",
	"gpt-4-turbo",
	"gpt-4-vision",
	"gpt-5",
	"o1",
	"o3",
	"o4",
	"computer-use",
}

// PDF / document file input (Responses input_file) — same family as vision for now.
var pdfModelPrefixes = []string{
	"gpt-4o",
	"gpt-4.1",
	"gpt-5",
	"o1",
	"o3",
	"o4",
}

func modelHasPrefix(model string, prefixes []string) bool {
	model = strings.TrimSpace(strings.ToLower(model))
	if model == "" {
		return false
	}
	for _, p := range prefixes {
		if strings.HasPrefix(model, p) {
			return true
		}
	}
	return false
}

// modelSupportsMIME is the OpenAI-strategy capability matrix for a model id.
func modelSupportsMIME(model, mimeType string) bool {
	mime := streaming.NormalizeMIME(mimeType)
	if streaming.IsTextMIME(mime) {
		return true
	}
	if strings.HasPrefix(mime, "image/") {
		return modelHasPrefix(model, visionModelPrefixes)
	}
	if mime == "application/pdf" {
		return modelHasPrefix(model, pdfModelPrefixes)
	}
	return false
}
