package meowcaller

import (
	"context"
	"testing"

	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const siblingCallID = "SIBLINGCALL"

func ownLID() types.JID { return types.JID{User: "111111111111111", Server: types.HiddenUserServer} }
func ownPN() types.JID  { return types.JID{User: "5500000000001", Server: types.DefaultUserServer} }

func ownDevice(jid types.JID, device uint16) types.JID {
	jid.Device = device
	return jid
}

// testEngineForOffers returns the engine of a Client whose store is device 7 of ownPN
// and ownLID, with the nodes it sends and the calls it hands to OnIncomingCall.
func testEngineForOffers() (*engine, *[]waBinary.Node, *[]*Call) {
	pn := ownDevice(ownPN(), 7)
	client := NewClient(whatsmeow.NewClient(&store.Device{ID: &pn, LID: ownDevice(ownLID(), 7)}, waLog.Noop))
	eng := client.eng
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return nil
	}
	var incoming []*Call
	client.OnIncomingCall(func(call *Call) { incoming = append(incoming, call) })
	return eng, &sent, &incoming
}

// siblingOffer is the offer one of this account's devices receives when the peer adds a
// participant to a call the account is in on its device 10. ownState is the account's
// roster state; silent adds the <silence> child.
func siblingOffer(ownState string, silent bool) *events.CallOffer {
	peer := peerJID()
	added := types.JID{User: "333333333333333", Server: types.HiddenUserServer}
	children := []waBinary.Node{{
		Tag: "group_info",
		Attrs: waBinary.Attrs{
			"call-id": siblingCallID, "connected-limit": "32", "media": "audio", "transaction-id": "11",
		},
		Content: []waBinary.Node{
			{
				Tag:   "user",
				Attrs: waBinary.Attrs{"jid": ownLID(), "state": ownState, "user_pn": ownPN()},
				Content: []waBinary.Node{{
					Tag: "device", Attrs: waBinary.Attrs{"jid": ownDevice(ownLID(), 10), "platform": "web"},
				}},
			},
			{
				Tag:   "user",
				Attrs: waBinary.Attrs{"jid": peer, "state": "connected"},
				Content: []waBinary.Node{{
					Tag: "device", Attrs: waBinary.Attrs{"jid": peer, "platform": "android"},
				}},
			},
			{
				Tag:     "user",
				Attrs:   waBinary.Attrs{"jid": added, "state": "outgoing"},
				Content: []waBinary.Node{{Tag: "device", Attrs: waBinary.Attrs{"jid": added}}},
			},
		},
	}}
	if silent {
		children = append(children, waBinary.Node{
			Tag: "silence", Attrs: waBinary.Attrs{"reason": "added_for_device_switching"},
		})
	}
	return &events.CallOffer{
		BasicCallMeta: types.BasicCallMeta{From: peer, CallCreator: peer, CallID: siblingCallID},
		Data: &waBinary.Node{
			Tag:     "offer",
			Attrs:   waBinary.Attrs{"call-creator": peer, "call-id": siblingCallID, "joinable": "1"},
			Content: children,
		},
	}
}

func requirePreacceptedAndHandedOver(t *testing.T, eng *engine, sent []waBinary.Node, incoming []*Call) {
	t.Helper()
	if len(sent) != 1 || len(sent[0].GetChildren()) != 1 || sent[0].GetChildren()[0].Tag != "preaccept" {
		t.Fatalf("sent = %#v, want one preaccept", sent)
	}
	if to := sent[0].Attrs["to"]; to != types.NewJID(siblingCallID, "call") {
		t.Fatalf("preaccept to = %v, want %s@call", to, siblingCallID)
	}
	if eng.lookup(siblingCallID) == nil {
		t.Fatal("the offer created no call entry")
	}
	if len(incoming) != 1 || incoming[0].id != siblingCallID {
		t.Fatalf("OnIncomingCall calls = %d, want one for the offer", len(incoming))
	}
}

func TestSilentOfferForACallThisAccountIsConnectedToIsIgnored(t *testing.T) {
	eng, sent, incoming := testEngineForOffers()

	eng.onOffer(siblingOffer("connected", true))

	if len(*sent) != 0 {
		t.Fatalf("sent %d nodes, want none for an ignored offer", len(*sent))
	}
	if eng.lookup(siblingCallID) != nil {
		t.Fatal("an ignored offer created a call entry")
	}
	if len(*incoming) != 0 {
		t.Fatalf("OnIncomingCall calls = %d, want none for an ignored offer", len(*incoming))
	}
}

func TestOfferWithoutSilenceForACallThisAccountIsConnectedToIsStillPreaccepted(t *testing.T) {
	eng, sent, incoming := testEngineForOffers()

	eng.onOffer(siblingOffer("connected", false))

	requirePreacceptedAndHandedOver(t, eng, *sent, *incoming)
}

func TestSilentOfferForACallThisAccountIsNotConnectedToIsStillPreaccepted(t *testing.T) {
	eng, sent, incoming := testEngineForOffers()

	eng.onOffer(siblingOffer("outgoing", true))

	requirePreacceptedAndHandedOver(t, eng, *sent, *incoming)
}

func TestOwnAccountConnected(t *testing.T) {
	other := peerJID()
	self := []types.JID{ownDevice(ownPN(), 7), ownDevice(ownLID(), 7)}
	cases := []struct {
		name         string
		participants []signaling.GroupCallParticipant
		self         []types.JID
		want         bool
	}{
		{
			name:         "by LID",
			participants: []signaling.GroupCallParticipant{{JID: ownLID(), State: "connected"}},
			self:         self,
			want:         true,
		},
		{
			name:         "by user_pn",
			participants: []signaling.GroupCallParticipant{{JID: ownLID(), PN: ownPN(), State: "connected"}},
			self:         []types.JID{ownDevice(ownPN(), 7)},
			want:         true,
		},
		{
			name:         "not connected",
			participants: []signaling.GroupCallParticipant{{JID: ownLID(), PN: ownPN(), State: "outgoing"}},
			self:         self,
		},
		{
			name:         "only others connected",
			participants: []signaling.GroupCallParticipant{{JID: other, State: "connected"}},
			self:         self,
		},
		{
			name:         "empty own phone number",
			participants: []signaling.GroupCallParticipant{{JID: other, State: "connected"}},
			self:         []types.JID{types.EmptyJID, ownDevice(ownLID(), 7)},
		},
		{
			name:         "no own JIDs",
			participants: []signaling.GroupCallParticipant{{JID: ownLID(), State: "connected"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ownAccountConnected(tc.participants, tc.self); got != tc.want {
				t.Fatalf("ownAccountConnected = %t, want %t", got, tc.want)
			}
		})
	}
}
