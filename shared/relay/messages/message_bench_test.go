package messages

import (
	"testing"
)

// benchTransportMsg builds a representative transport message with a ~1400 byte payload.
func benchTransportMsg(tb testing.TB) []byte {
	tb.Helper()
	peerID := HashID("abdFAaBcawquEiCMzAabYosuUaGLtSNhKxz+")
	payload := make([]byte, 1400)
	msg, err := MarshalTransportMsg(peerID, payload)
	if err != nil {
		tb.Fatalf("failed to marshal transport message: %v", err)
	}
	return msg
}

// BenchmarkUnmarshalTransportID measures the allocating (*PeerID) unmarshal on the hot path.
func BenchmarkUnmarshalTransportID(b *testing.B) {
	msg := benchTransportMsg(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, err := UnmarshalTransportID(msg)
		if err != nil {
			b.Fatal(err)
		}
		_ = id
	}
}

// BenchmarkUnmarshalTransportIDInto measures the zero-allocation in-place unmarshal.
func BenchmarkUnmarshalTransportIDInto(b *testing.B) {
	msg := benchTransportMsg(b)
	var id PeerID
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !UnmarshalTransportIDInto(msg, &id) {
			b.Fatal("failed to unmarshal transport id")
		}
	}
}
