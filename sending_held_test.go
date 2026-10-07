package meowcaller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func skipSendingHeldStub(t *testing.T) {
	t.Helper()
	t.Skip("blocked: engine/sending-held is a stub; enable when implemented")
}

// relayPackets counts what a loopback relay reads: media (RTP and SRTCP, version 2) and
// everything else (the allocate and the pings).
type relayPackets struct {
	media, other atomic.Int64
}

func (r *relayPackets) record(packet []byte) {
	if len(packet) > 0 && packet[0]&0xC0 == 0x80 {
		r.media.Add(1)
		return
	}
	r.other.Add(1)
}

func waitForMedia(t *testing.T, packets *relayPackets) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for packets.media.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no media reached the relay")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runRecordedMedia(t *testing.T, opts ...Option) (*MediaCall, *relayPackets) {
	t.Helper()
	packets := &relayPackets{}
	session, _ := recordingRelaySession(t, 0, packets.record)
	mc, err := RunMedia(context.Background(), session, opts...)
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	t.Cleanup(func() { mc.Stop(); waitMediaDone(t, mc) })
	return mc, packets
}

func TestSendingIsHeld(t *testing.T) {
	skipSendingHeldStub(t)
	if sendingIsHeld(nil) {
		t.Fatal("a nil flag holds the sending")
	}
	var held atomic.Bool
	held.Store(true)
	if !sendingIsHeld(&held) {
		t.Fatal("a flag storing true does not hold the sending")
	}
	held.Store(false)
	if sendingIsHeld(&held) {
		t.Fatal("a flag storing false holds the sending")
	}
}

func TestWithSendingHeldReachesRunMedia(t *testing.T) {
	skipSendingHeldStub(t)
	mc, err := RunMedia(context.Background(), silentRelaySession(t), WithSendingHeld())
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	t.Cleanup(func() { mc.Stop(); waitMediaDone(t, mc) })

	mc.eng.mu.Lock()
	held := mc.eng.calls["CID"].sendHeld
	mc.eng.mu.Unlock()
	if !sendingIsHeld(held) {
		t.Fatal("RunMedia with WithSendingHeld does not hold the sending")
	}
}

func TestRunMediaWithSendingHeldSendsNoMedia(t *testing.T) {
	skipSendingHeldStub(t)
	_, packets := runRecordedMedia(t, WithSendingHeld())

	// The keepalive runs every second and the SRTCP reports every 1.5 s: past 2 s both
	// have had their turn.
	time.Sleep(2 * time.Second)

	if packets.other.Load() == 0 {
		t.Fatal("the relay leg did not come up")
	}
	if n := packets.media.Load(); n != 0 {
		t.Fatalf("%d media packets reached the relay while the sending was held", n)
	}
}

func TestStartSendingStartsTheMedia(t *testing.T) {
	skipSendingHeldStub(t)
	mc, packets := runRecordedMedia(t, WithSendingHeld())
	time.Sleep(200 * time.Millisecond)

	mc.StartSending()

	waitForMedia(t, packets)
}

func TestStartSendingTwiceKeepsSending(t *testing.T) {
	skipSendingHeldStub(t)
	mc, packets := runRecordedMedia(t, WithSendingHeld())

	mc.StartSending()
	mc.StartSending()

	waitForMedia(t, packets)
}

func TestStartSendingWithoutTheHoldChangesNothing(t *testing.T) {
	skipSendingHeldStub(t)
	mc, packets := runRecordedMedia(t)

	mc.StartSending()

	waitForMedia(t, packets)
}

func TestRunMediaWithoutTheHoldSendsAtOnce(t *testing.T) {
	_, packets := runRecordedMedia(t)

	waitForMedia(t, packets)
}
