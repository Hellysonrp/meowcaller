package meowcaller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func skipRelayTimeoutStub(t *testing.T) {
	t.Helper()
	t.Skip("blocked: engine/relay-timeout is a stub; enable when implemented")
}

func runWatchRelay(ctx context.Context, rx *atomic.Uint64, timeout time.Duration) (*atomic.Int32, chan struct{}) {
	expired := &atomic.Int32{}
	done := make(chan struct{})
	go func() {
		watchRelay(ctx, rx, timeout, 5*time.Millisecond, func() { expired.Add(1) })
		close(done)
	}()
	return expired, done
}

func waitWatchRelay(t *testing.T, done chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchRelay did not return")
	}
}

func TestWatchRelayExpiresOnceWhenRelayIsSilent(t *testing.T) {
	skipRelayTimeoutStub(t)
	var rx atomic.Uint64
	expired, done := runWatchRelay(context.Background(), &rx, 30*time.Millisecond)

	waitWatchRelay(t, done)

	if got := expired.Load(); got != 1 {
		t.Fatalf("expired calls = %d, want 1", got)
	}
}

func TestWatchRelayStaysQuietWhileRelaySends(t *testing.T) {
	skipRelayTimeoutStub(t)
	var rx atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	expired, done := runWatchRelay(ctx, &rx, 100*time.Millisecond)

	for range 60 {
		rx.Add(1)
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	waitWatchRelay(t, done)

	if got := expired.Load(); got != 0 {
		t.Fatalf("expired calls = %d while the relay kept sending, want 0", got)
	}
}

func TestWatchRelayReturnsWhenStoppedFirst(t *testing.T) {
	skipRelayTimeoutStub(t)
	var rx atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	expired, done := runWatchRelay(ctx, &rx, time.Hour)

	time.Sleep(20 * time.Millisecond)
	cancel()
	waitWatchRelay(t, done)

	if got := expired.Load(); got != 0 {
		t.Fatalf("expired calls = %d after the media stopped, want 0", got)
	}
}

func TestWithRelayTimeoutReachesNewClient(t *testing.T) {
	skipRelayTimeoutStub(t)
	wa := whatsmeow.NewClient(&store.Device{}, waLog.Noop)

	c := NewClient(wa, WithRelayTimeout(7*time.Second))

	if c.relayTimeout != 7*time.Second {
		t.Fatalf("relayTimeout = %s, want 7s", c.relayTimeout)
	}
}

func TestWithRelayTimeoutReachesRunMedia(t *testing.T) {
	skipRelayTimeoutStub(t)
	mc, err := RunMedia(context.Background(), silentRelaySession(t), WithRelayTimeout(7*time.Second))
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	t.Cleanup(func() { mc.Stop(); waitMediaDone(t, mc) })

	if mc.eng.c.relayTimeout != 7*time.Second {
		t.Fatalf("relayTimeout = %s, want 7s", mc.eng.c.relayTimeout)
	}
}
