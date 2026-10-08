package meowcaller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

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

// waitForRelayLeg waits for the relay to read the allocate, failing if the media ends
// first.
func waitForRelayLeg(t *testing.T, mc *MediaCall, packets *relayPackets) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for packets.other.Load() == 0 {
		select {
		case <-mc.Done():
			t.Fatalf("the media ended before the relay leg came up: %v", mc.Err())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the relay leg did not come up")
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
	mc, packets := runRecordedMedia(t, WithSendingHeld())
	waitForRelayLeg(t, mc, packets)

	// From the allocate on, the keepalive runs every second and the SRTCP reports every
	// 1.5 s: past 2 s both have had their turn.
	time.Sleep(2 * time.Second)

	if n := packets.media.Load(); n != 0 {
		t.Fatalf("%d media packets reached the relay while the sending was held", n)
	}
}

func TestAHeldCallRefusesAReaction(t *testing.T) {
	mc, packets := runRecordedMedia(t, WithSendingHeld())
	waitForRelayLeg(t, mc, packets)

	if err := mc.call.SendReaction("x"); !errors.Is(err, errSendingHeld) {
		t.Fatalf("SendReaction = %v, want errSendingHeld", err)
	}
	if n := packets.media.Load(); n != 0 {
		t.Fatalf("%d media packets reached the relay while the sending was held", n)
	}
}

func TestAHeldCallRefusesVideo(t *testing.T) {
	mc, packets := runRecordedMedia(t, WithSendingHeld())
	waitForRelayLeg(t, mc, packets)

	if err := mc.call.SendVideo([]byte{0, 0, 0, 1, 0x65, 0x88}); !errors.Is(err, errSendingHeld) {
		t.Fatalf("SendVideo = %v, want errSendingHeld", err)
	}
	if n := packets.media.Load(); n != 0 {
		t.Fatalf("%d media packets reached the relay while the sending was held", n)
	}
}

// countingSource is an AudioSource of silence that counts the frames read from it.
type countingSource struct{ reads atomic.Int64 }

func (s *countingSource) ReadFrame() ([]float32, error) {
	s.reads.Add(1)
	return make([]float32, FrameSamples), nil
}

func (s *countingSource) Close() error { return nil }

// A held call keeps pulling its Player's frames, so a live source does not back up into
// latency for when the sending starts.
func TestAHeldCallDrainsItsPlayer(t *testing.T) {
	mc, packets := runRecordedMedia(t, WithSendingHeld())
	source := &countingSource{}
	mc.Play(source)

	deadline := time.Now().Add(3 * time.Second)
	for source.reads.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("%d frames read from the player while held, want at least 3", source.reads.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := packets.media.Load(); n != 0 {
		t.Fatalf("%d media packets reached the relay while the sending was held", n)
	}
}

func TestStartSendingStartsTheMedia(t *testing.T) {
	mc, packets := runRecordedMedia(t, WithSendingHeld())
	time.Sleep(200 * time.Millisecond)

	mc.StartSending()

	waitForMedia(t, packets)
}

func TestStartSendingTwiceKeepsSending(t *testing.T) {
	mc, packets := runRecordedMedia(t, WithSendingHeld())

	mc.StartSending()
	mc.StartSending()

	waitForMedia(t, packets)
}

func TestStartSendingWithoutTheHoldChangesNothing(t *testing.T) {
	mc, packets := runRecordedMedia(t)

	mc.StartSending()

	waitForMedia(t, packets)
}

func TestRunMediaWithoutTheHoldSendsAtOnce(t *testing.T) {
	_, packets := runRecordedMedia(t)

	waitForMedia(t, packets)
}
