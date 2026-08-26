package tacklr

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func toolCall(id, name, args string) ToolCall {
	return ToolCall{ID: id, CallID: id, Name: name, Arguments: args}
}

func TestTagModelAfterToolsError_wrapsProviderFailures(t *testing.T) {
	// Arrange
	base := errors.New("upstream failed")
	wrapped := fmt.Errorf("%w: already", ErrModelAfterTools)

	// Act
	fromError := tagModelAfterToolsError(LLMResponseChunk{Error: base})
	fromWrapped := tagModelAfterToolsError(LLMResponseChunk{Error: wrapped})
	fromContent := tagModelAfterToolsError(LLMResponseChunk{Content: "provider said no"})

	// Assert
	if !errors.Is(fromError.Error, ErrModelAfterTools) || fromError.Content == "" {
		t.Fatalf("from error = %+v", fromError)
	}
	if !errors.Is(fromWrapped.Error, ErrModelAfterTools) || strings.Count(fromWrapped.Error.Error(), "model request failed") != 1 {
		t.Fatalf("double wrap = %v", fromWrapped.Error)
	}
	if !errors.Is(fromContent.Error, ErrModelAfterTools) || !strings.Contains(fromContent.Content, "provider said no") {
		t.Fatalf("from content = %+v", fromContent)
	}
}
