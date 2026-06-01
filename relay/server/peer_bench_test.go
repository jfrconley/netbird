package server

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/netbirdio/netbird/relay/metrics"
	"github.com/netbirdio/netbird/relay/server/store"
	"github.com/netbirdio/netbird/shared/relay/healthcheck"
	"github.com/netbirdio/netbird/shared/relay/messages"
)

// benchConn is a no-op listener.Conn used to isolate the forwarding CPU cost from real I/O.
type benchConn struct{}

func (benchConn) Read(context.Context, []byte) (int, error)      { return 0, nil }
func (benchConn) Write(_ context.Context, b []byte) (int, error) { return len(b), nil }
func (benchConn) RemoteAddr() net.Addr                           { return &net.TCPAddr{} }
func (benchConn) Close() error                                   { return nil }

// BenchmarkHandleTransportMsg measures the per-packet forwarding path
// (header parse, destination lookup, source-ID rewrite, write, metrics) with a no-op meter and conn.
func BenchmarkHandleTransportMsg(b *testing.B) {
	meter := noop.NewMeterProvider().Meter("")
	m, err := metrics.NewMetrics(context.Background(), meter)
	if err != nil {
		b.Fatalf("failed to create metrics: %v", err)
	}

	st := store.NewStore()
	notifier := store.NewPeerNotifier()

	srcID := messages.HashID("bench-src")
	dstID := messages.HashID("bench-dst")
	src := NewPeer(m, srcID, benchConn{}, st, notifier)
	dst := NewPeer(m, dstID, benchConn{}, st, notifier)
	st.AddPeer(dst)

	hc := healthcheck.NewSender(src.log)

	payload := make([]byte, 1400)
	msg, err := messages.MarshalTransportMsg(dstID, payload)
	if err != nil {
		b.Fatalf("failed to marshal transport message: %v", err)
	}

	b.SetBytes(int64(len(msg)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src.handleMsgType(src.ctx, messages.MsgTypeTransport, hc, len(msg), msg)
	}
}
