package adapter

import (
	"context"
	"sync"

	"github.com/ryanaldo34/tacklr"
)

// InboxSafe reports whether queued user and job messages may enter the window.
// Drain only when this is true. Pass leftover unstarted tools as runnable
// (they are unpaired function_calls). Parked HITL is never safe.
func InboxSafe(runnable int, parked bool) bool {
	return runnable == 0 && !parked
}

// Inbox is the wait-loop FIFO for steers and auto-collected job results.
// All methods are safe for concurrent callers. In-process uses this so the
// session loop (Prompt), the turn goroutine (drain), and Cancel/Close cannot
// race the slice. Hold sessionProc.mu around terminal checks plus Push so a
// Cancel Drop cannot lose the ordering with Queue.
//
// Temporal workflows must not use Inbox: replay is single-threaded and must
// not park on sync.Mutex. Use AppendMessages / TakeMessages on a local slice.
type Inbox struct {
	mu   sync.Mutex
	msgs []*tacklr.Message
}

// Push appends non-nil messages in call order.
func (b *Inbox) Push(msgs ...*tacklr.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgs = AppendMessages(b.msgs, msgs...)
}

// Take removes and returns all queued messages.
func (b *Inbox) Take() []*tacklr.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.msgs
	b.msgs = nil
	return out
}

// Drop discards unread messages (Cancel / Close).
func (b *Inbox) Drop() {
	b.mu.Lock()
	b.msgs = nil
	b.mu.Unlock()
}

// AppendMessages is the unlocked FIFO append. Temporal wait-loop only.
func AppendMessages(box []*tacklr.Message, msgs ...*tacklr.Message) []*tacklr.Message {
	for _, m := range msgs {
		if m != nil {
			box = append(box, m)
		}
	}
	return box
}

// TakeMessages is the unlocked FIFO take. Temporal wait-loop only.
func TakeMessages(box *[]*tacklr.Message) []*tacklr.Message {
	if box == nil || len(*box) == 0 {
		return nil
	}
	out := *box
	*box = nil
	return out
}

// UserFromPrompt is the Prompt text or UserMessage for the window.
func UserFromPrompt(text string, msg *tacklr.Message) *tacklr.Message {
	if msg != nil {
		return msg
	}
	return &tacklr.Message{Role: tacklr.RoleUser, Content: text}
}

// AbsorbAll writes msgs into the window in order. consumed is how many
// absorb calls ran (including a failed one). Absorb failure is terminal
// for the batch: leftover messages are not put back.
func AbsorbAll(ctx context.Context, absorb func(context.Context, *tacklr.Message, chan tacklr.StreamEvent) error, msgs []*tacklr.Message, out chan tacklr.StreamEvent) (consumed int, err error) {
	for i, msg := range msgs {
		if err := absorb(ctx, msg, out); err != nil {
			return i + 1, err
		}
	}
	return len(msgs), nil
}
