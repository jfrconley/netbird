package messages

import (
	"bytes"
	"testing"
)

var benchPeerID = HashID("bench-peer")

// BenchmarkMarshalTransportMsg measures the current client write path, which
// allocates a fresh buffer for every outbound packet.
func BenchmarkMarshalTransportMsg(b *testing.B) {
	payload := make([]byte, 1400) // typical MTU-sized WireGuard packet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg, err := MarshalTransportMsg(benchPeerID, payload)
		if err != nil {
			b.Fatal(err)
		}
		_ = msg
	}
}

// BenchmarkMarshalTransportMsgInto measures the pooled write path: marshalling
// into a reused buffer should report 0 allocs/op.
func BenchmarkMarshalTransportMsgInto(b *testing.B) {
	payload := make([]byte, 1400)
	buf := make([]byte, MaxMessageSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg, err := MarshalTransportMsgInto(buf[:0], benchPeerID, payload)
		if err != nil {
			b.Fatal(err)
		}
		_ = msg
	}
}

// TestMarshalTransportMsgInto verifies the pooled marshal produces a message
// identical to the allocating MarshalTransportMsg and round-trips correctly,
// including when the supplied buffer is too small (forcing a fresh allocation).
func TestMarshalTransportMsgInto(t *testing.T) {
	payload := []byte("hello relay payload")

	want, err := MarshalTransportMsg(benchPeerID, payload)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		buf  []byte
	}{
		{"large enough", make([]byte, MaxMessageSize)},
		{"too small", make([]byte, 4)},
		{"nil", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MarshalTransportMsgInto(tc.buf[:0], benchPeerID, payload)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("marshalled message mismatch:\n got=%v\nwant=%v", got, want)
			}

			id, gotPayload, err := UnmarshalTransportMsg(got)
			if err != nil {
				t.Fatal(err)
			}
			if *id != benchPeerID {
				t.Fatalf("peer ID mismatch: got %v want %v", id, benchPeerID)
			}
			if !bytes.Equal(gotPayload, payload) {
				t.Fatalf("payload mismatch: got %q want %q", gotPayload, payload)
			}
		})
	}
}
