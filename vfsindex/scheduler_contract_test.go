package vfsindex

import (
	"errors"
	"testing"
)

func TestAsyncScheduler_reportsQueueAndClosedOutcomes(t *testing.T) {
	// Arrange
	var events []SchedulerEvent
	scheduler := &AsyncScheduler{
		QueueCap: 1,
		pending:  map[string]struct{}{"/queued": {}},
		wake:     make(chan struct{}, 1),
	}
	scheduler.SetObserver(func(event SchedulerEvent) {
		events = append(events, event)
	})

	// Act
	queueErr := scheduler.Notify(t.Context(), "/dropped", ReasonSync)
	scheduler.mu.Lock()
	scheduler.closed = true
	scheduler.mu.Unlock()
	closedErr := scheduler.Notify(t.Context(), "/closed", ReasonExplicit)

	// Assert
	if !errors.Is(queueErr, ErrQueueFull) || !errors.Is(closedErr, ErrSchedulerClosed) {
		t.Fatalf("queue error = %v closed error = %v", queueErr, closedErr)
	}
	if len(events) != 2 || !errors.Is(events[0].Err, ErrQueueFull) || !errors.Is(events[1].Err, ErrSchedulerClosed) {
		t.Fatalf("events = %#v", events)
	}
}
