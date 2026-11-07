package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// pressureRatio is the fraction of the window's size a commit may fill
// before the window collapses into one summary. A collapse leaves the
// window small, so the exact value matters little.
const pressureRatio = 0.85

// window is the context window: the committed messages the model sees, the
// provider's count of the last request on them, and the rules that keep
// every request under the model's context window. It is the only owner of
// the messages: others read a copy, or ask it to change them. It converts
// them to checkpoint rows and back, and sizes a request from its count.
type window struct {
	model Model
	host  windowHost
	// size is MaxWindowSize, in tokens.
	size int

	mu   sync.RWMutex
	msgs []*message.Message
	// count is the provider's count of the last request on msgs. It
	// survives an append, because that request is a prefix of the next
	// one; a window that is replaced forgets it.
	count requestCount
	// cannotCount is set once the model's CountTokens said it cannot
	// count: requests are then sized from count alone.
	cannotCount bool
}

// windowHost is what the window uses that the harness owns: the parts of
// a request other than its messages, the model tasks that write text from
// a window, and where messages and results that leave the window go.
type windowHost interface {
	// request is a model request on msgs, with the harness's system
	// prompt, tools and cache key.
	request(msgs []*message.Message) ModelRequest
	// summarize is the model's summary of msgs.
	summarize(ctx context.Context, msgs []*message.Message) (string, error)
	// archive saves msgs for search. It reports whether they are kept.
	archive(ctx context.Context, msgs []*message.Message) (bool, error)
	// keep stores a cut result's full text, and returns where it is, or
	// "" when nothing keeps it.
	keep(ctx context.Context, callID, text string) (string, error)
}

// requestCount is a provider's input token count for one model request,
// and how many window messages that request carried. Zero is no count.
type requestCount struct {
	Tokens   int `json:"tokens"`
	Messages int `json:"messages"`
}

func newWindow(model Model, host windowHost, size int) *window {
	return &window{model: model, host: host, size: size}
}

// Messages is a copy of the window.
func (w *window) Messages() []*message.Message {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return slices.Clone(w.msgs)
}

// afterTools reports whether the window ends with a tool round: the next
// request answers tool results.
func (w *window) afterTools() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.msgs) > 0 && w.msgs[len(w.msgs)-1].Role == message.RoleTool
}

// set replaces the window with msgs and their count.
func (w *window) set(msgs []*message.Message, count requestCount) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs, w.count = msgs, count
}

// Add appends round as it is, for a round whose window effect replaces
// the window next.
func (w *window) Add(round ...*message.Message) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, round...)
}

// Fit appends round. counted is the provider's count of the request the
// round's reply answered, which carried the window as it is; zero is no
// count. Over the pressure the window first collapses: everything after
// the protected prefix (an earlier summary too) is archived and replaced
// by one summary. The window is then small, so the next rounds append
// without collapsing. A round still over the window's size has its
// largest results cut.
func (w *window) Fit(ctx context.Context, round []*message.Message, counted int) error {
	msgs, count := w.Messages(), w.counted()
	if counted > 0 {
		count = requestCount{Tokens: counted, Messages: len(msgs)}
	}
	threshold := int(float64(w.size) * pressureRatio)
	if w.bound(count, msgs, round) < threshold {
		w.set(slices.Concat(msgs, round), count)
		return nil
	}
	n, err := w.measure(ctx, count, msgs, round)
	if err != nil {
		return err
	}
	if n > threshold && len(msgs) > protectedPrefixLen(msgs) {
		if msgs, err = w.collapse(ctx, msgs, len(msgs)); err != nil {
			return err
		}
		count = requestCount{}
		round = withoutReasoning(round)
		if n, err = w.measure(ctx, count, msgs, round); err != nil {
			return err
		}
	}
	if n > w.size {
		if round, err = w.trim(ctx, count, msgs, round, n); err != nil {
			return err
		}
	}
	w.set(slices.Concat(msgs, round), count)
	return nil
}

// Refit collapses the window after the provider refused a request on it
// as over the context window, although its count or bound was under. The
// messages after the protected prefix that the last counted request
// carried become one summary: the provider took that request, so the
// summary request fits. The messages after it stay. With no count,
// everything after the prefix becomes the summary. A window with nothing
// to collapse fails with session.ErrContextLength.
func (w *window) Refit(ctx context.Context) error {
	msgs, count := w.Messages(), w.counted()
	keep := protectedPrefixLen(msgs)
	end := count.Messages
	if end <= keep {
		end = len(msgs)
	}
	if end <= keep {
		return fmt.Errorf("%w: the window has nothing to collapse", session.ErrContextLength)
	}
	msgs, err := w.collapse(ctx, msgs, end)
	if err != nil {
		return err
	}
	w.set(msgs, requestCount{})
	return nil
}

// InstallPlan sets the window to the first user message and the plan
// document.
func (w *window) InstallPlan(plan string) error {
	if plan == "" {
		return fmt.Errorf("install plan document: no plan document")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs, w.count = []*message.Message{w.msgs[0], planDocument(plan)}, requestCount{}
	return nil
}

// HandOff archives the window after the first user message, then replaces
// it with that message, the plan document (when there is one), the handoff
// that write makes of the window, and, while todos are open, a nudge to
// continue.
func (w *window) HandOff(ctx context.Context, plan string, open bool, write func(context.Context, []*message.Message) (string, error)) error {
	msgs := w.Messages()
	if _, err := w.host.archive(ctx, msgs[min(len(msgs), 1):]); err != nil {
		return err
	}
	handoff, err := write(ctx, msgs)
	if err != nil {
		return err
	}
	next := []*message.Message{msgs[0]}
	if plan != "" {
		next = append(next, planDocument(plan))
	}
	next = append(next, &message.Message{Role: message.RoleDeveloper, Content: handoff})
	if open {
		next = append(next, &message.Message{Role: message.RoleDeveloper, Content: continuePlanNudge})
	}
	w.set(next, requestCount{})
	return nil
}

func (w *window) counted() requestCount {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.count
}

// collapse archives msgs[keep:end] and replaces it with one summary of
// msgs[:end], where keep is the protected prefix.
func (w *window) collapse(ctx context.Context, msgs []*message.Message, end int) ([]*message.Message, error) {
	slog.InfoContext(ctx, "context window over pressure, collapsing it into a summary",
		"area", telemetry.AreaModelTasks, "max_size", w.size, "messages", end)
	keep := protectedPrefixLen(msgs)
	kept, err := w.host.archive(ctx, msgs[keep:end])
	if err != nil {
		return nil, err
	}
	text, err := w.host.summarize(ctx, msgs[:end])
	if err != nil {
		return nil, err
	}
	content := contextSummaryPrefix + text
	if kept {
		content += "\n\n" + searchableHistoryNote
	}
	return slices.Concat(msgs[:keep], []*message.Message{{Role: message.RoleDeveloper, Content: content}}, withoutReasoning(msgs[end:])), nil
}

// withoutReasoning is msgs without their reasoning. A collapse drops the
// reasoning of the messages it keeps: that reasoning is about messages
// the summary replaced, which the model no longer sees.
func withoutReasoning(msgs []*message.Message) []*message.Message {
	return slices.DeleteFunc(slices.Clone(msgs), func(m *message.Message) bool { return m != nil && m.Role == message.RoleReasoning })
}

// bound is an upper bound on the input tokens of a request with msgs and
// round: count's tokens, plus the encoded size of the messages after the
// ones it counted. With no count, it is the encoded size of the whole
// request (ModelRequest.encodedSize).
func (w *window) bound(count requestCount, msgs, round []*message.Message) int {
	if count.Tokens == 0 {
		return w.host.request(slices.Concat(msgs, round)).encodedSize()
	}
	return count.Tokens + messagesSize(msgs[count.Messages:]) + messagesSize(round)
}

// measure is the input tokens of a request with msgs and round: the
// provider's count, or the bound when the provider cannot count.
func (w *window) measure(ctx context.Context, count requestCount, msgs, round []*message.Message) (int, error) {
	if !w.cannotCount {
		n, err := w.model.CountTokens(ctx, w.host.request(slices.Concat(msgs, round)))
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, ErrCannotCount) {
			return 0, fmt.Errorf("count tokens: %w", err)
		}
		slog.InfoContext(ctx, "the model cannot count tokens; requests are sized from the usage the provider reports",
			"area", telemetry.AreaModelTasks, "error", err)
		w.cannotCount = true
	}
	return w.bound(count, msgs, round), nil
}

// trim cuts the round's largest results until the window and round fit
// the window's size. Before a result's first cut, its full text is kept
// when the host keeps results, and the cut result says where.
func (w *window) trim(ctx context.Context, count requestCount, msgs, round []*message.Message, n int) ([]*message.Message, error) {
	round = slices.Clone(round)
	full := map[int]string{}
	kept := map[int]int{}
	where := map[int]string{}
	size := func(i int) int {
		if k, ok := kept[i]; ok {
			return k
		}
		return len(round[i].Content)
	}
	for n > w.size {
		i := -1
		for j, m := range round {
			if m.Role == message.RoleTool && size(j) > 0 && (i < 0 || size(j) > size(i)) {
				i = j
			}
		}
		if i < 0 {
			return nil, fmt.Errorf("%w: the round needs %d tokens and the context window holds %d", ErrFailed, n, w.size)
		}
		if _, ok := full[i]; !ok {
			full[i] = round[i].Content
			path, err := w.host.keep(ctx, round[i].ToolCallID, full[i])
			if err != nil {
				return nil, fmt.Errorf("stage full tool result: %w", err)
			}
			where[i] = path
		}
		// Scale the result by how far over the window is; n > size, so
		// each pass keeps less.
		keep := int(int64(size(i)) * int64(w.size) / int64(n))
		for keep > 0 && !utf8.RuneStart(full[i][keep]) {
			keep--
		}
		kept[i] = keep
		cut := *round[i]
		cut.Content = full[i][:keep] + truncationNote(keep, len(full[i]), where[i])
		round[i] = &cut
		var err error
		if n, err = w.measure(ctx, count, msgs, round); err != nil {
			return nil, err
		}
	}
	return round, nil
}

// rows are the window as checkpoint rows: one per message, keyed by its
// position so an append writes only the new positions, and the count.
func (w *window) rows() []session.CheckpointEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()
	rows := make([]session.CheckpointEntry, 0, len(w.msgs)+1)
	for i, m := range w.msgs {
		raw, _ := json.Marshal(m) // a Message is plain data
		rows = append(rows, session.CheckpointEntry{Kind: rowMessage, Key: position(i), Value: raw})
	}
	if w.count.Tokens > 0 {
		raw, _ := json.Marshal(w.count) // two ints
		rows = append(rows, session.CheckpointEntry{Kind: rowCount, Value: raw})
	}
	return rows
}

// restore replaces the window with the one rows hold, in key order.
func (w *window) restore(rows []session.CheckpointEntry) error {
	var msgs []*message.Message
	var count requestCount
	for _, r := range rows {
		var err error
		if r.Kind == rowCount {
			err = json.Unmarshal(r.Value, &count)
		} else {
			m := new(message.Message)
			err = json.Unmarshal(r.Value, m)
			msgs = append(msgs, m)
		}
		if err != nil {
			return fmt.Errorf("checkpoint %s %s: %w", r.Kind, r.Key, err)
		}
	}
	if err := ValidateMessages(msgs); err != nil {
		return fmt.Errorf("checkpoint: invalid context window: %w", err)
	}
	w.set(msgs, count)
	return nil
}

// messagesSize is the encoded size in bytes of msgs.
func messagesSize(msgs []*message.Message) int {
	n := 0
	for _, m := range msgs {
		raw, _ := json.Marshal(m) // a Message is plain data
		n += len(raw)
	}
	return n
}

// protectedPrefixLen is what a collapse keeps: the first user message, and
// the plan document after it.
func protectedPrefixLen(msgs []*message.Message) int {
	if len(msgs) > 1 && IsPlanDocument(msgs[1]) {
		return 2
	}
	return min(len(msgs), 1)
}

// truncationNote ends a cut result: how much of it is left, and where the
// full output is when it was kept.
func truncationNote(kept, total int, path string) string {
	if path == "" {
		return fmt.Sprintf("\n\n[Output truncated to fit the context window: kept the first %d of %d bytes. The full output was not kept.]", kept, total)
	}
	return fmt.Sprintf("\n\n[Output truncated to fit the context window: kept the first %d of %d bytes. The full output is at %s; read it with start and end.]", kept, total, path)
}

// planDocumentPrefix identifies durable plan messages so a collapse keeps
// them.
const planDocumentPrefix = "PROJECT PLAN\n────────────\n"

// IsPlanDocument reports the durable PROJECT PLAN developer message.
func IsPlanDocument(m *message.Message) bool {
	return m != nil && strings.HasPrefix(m.Content, planDocumentPrefix)
}

func planDocument(raw string) *message.Message {
	return &message.Message{Role: message.RoleDeveloper, Content: planDocumentPrefix + raw}
}

// contextSummaryPrefix heads the summary a collapse writes.
const contextSummaryPrefix = "CONVERSATION SUMMARY\n────────────\n"

// searchableHistoryNote ends a summary when the collapsed messages are
// kept.
const searchableHistoryNote = "The full earlier conversation is saved. Use the search tool to find its exact messages, and open to read one."

// continuePlanNudge ends a handoff while todos are open, so the next turn
// keeps calling tools instead of ending.
const continuePlanNudge = `The plan still has incomplete todos. Continue executing now: work the in-progress todo (or the next pending one), call tools as needed, and do not stop for user confirmation. Do not restate the handoff; act on the next todo.`
