package meowcaller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

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
	var rx atomic.Uint64
	expired, done := runWatchRelay(context.Background(), &rx, 30*time.Millisecond)

	waitWatchRelay(t, done)

	if got := expired.Load(); got != 1 {
		t.Fatalf("expired calls = %d, want 1", got)
	}
}

func TestWatchRelayStaysQuietWhileRelaySends(t *testing.T) {
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

func TestRunMediaRelayTimeoutEndsSilentConnectedMedia(t *testing.T) {
	session, _ := quietRelaySession(t)
	mc, err := RunMedia(context.Background(), session, WithRelayTimeout(300*time.Millisecond))
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}

	select {
	case <-mc.Done():
	case <-time.After(15 * time.Second):
		mc.Stop()
		waitMediaDone(t, mc)
		t.Fatal("the relay timeout did not end the media")
	}

	if err := mc.Err(); !errors.Is(err, ErrRelayTimeout) {
		t.Fatalf("Err = %v, want ErrRelayTimeout", err)
	}
}

func TestRelayWatchdogEndsWithTheMediaLoop(t *testing.T) {
	session, _ := loopbackRelaySession(t, 3)
	eng, _, _ := testEngineWithIncomingCall()
	logs := &syncBuffer{}
	eng.c.log = zerolog.New(logs).Level(zerolog.DebugLevel)
	eng.c.relayTimeout = 300 * time.Millisecond
	m := eng.calls["CID"]
	m.callKey = session.CallKey
	m.relay = session.Relay.relayData()
	m.selfLID = session.SelfLID
	m.peerLID = session.PeerLID
	t.Cleanup(func() { eng.finishCall("CID", "test end") })

	eng.maybeStartMedia("CID")
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), `"message":"media ended"`) {
		if time.Now().After(deadline) {
			t.Fatalf("the media loop did not end when the relay hung up; logs:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(600 * time.Millisecond)

	if strings.Contains(logs.String(), "relay sent nothing within the relay timeout") {
		t.Fatalf("the watchdog fired after the media loop had ended; logs:\n%s", logs.String())
	}
}

func TestWithRelayTimeoutReachesNewClient(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{}, waLog.Noop)

	c := NewClient(wa, WithRelayTimeout(7*time.Second))

	if c.relayTimeout != 7*time.Second {
		t.Fatalf("relayTimeout = %s, want 7s", c.relayTimeout)
	}
}

func TestWithRelayTimeoutReachesRunMedia(t *testing.T) {
	mc, err := RunMedia(context.Background(), silentRelaySession(t), WithRelayTimeout(7*time.Second))
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	t.Cleanup(func() { mc.Stop(); waitMediaDone(t, mc) })

	if mc.eng.c.relayTimeout != 7*time.Second {
		t.Fatalf("relayTimeout = %s, want 7s", mc.eng.c.relayTimeout)
	}
}
