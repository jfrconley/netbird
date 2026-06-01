package ws

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
)

// BenchmarkWSConnWrite measures the server-side write path over a real coder/websocket
// connection while the client continuously drains, so flow control does not block writes.
// It directly exposes per-write timer/context overhead.
func BenchmarkWSConnWrite(b *testing.B) {
	serverConnCh := make(chan *Conn, 1)
	done := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			return
		}
		serverConnCh <- NewConn(wsConn, &net.TCPAddr{})
		<-done // keep the handler (and the underlying conn) alive for the benchmark duration
	})

	srv := httptest.NewServer(handler)
	defer srv.Close()

	ctx := context.Background()
	clientConn, _, err := websocket.Dial(ctx, srv.URL, nil)
	if err != nil {
		b.Fatalf("failed to dial: %v", err)
	}
	clientConn.SetReadLimit(-1)
	defer func() { _ = clientConn.Close(websocket.StatusNormalClosure, "") }()

	serverConn := <-serverConnCh
	defer close(done)

	drainStop := make(chan struct{})
	go func() {
		for {
			select {
			case <-drainStop:
				return
			default:
			}
			if _, _, err := clientConn.Read(ctx); err != nil {
				return
			}
		}
	}()
	defer close(drainStop)

	payload := make([]byte, 1400)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := serverConn.Write(ctx, payload); err != nil {
			b.Fatalf("write failed: %v", err)
		}
	}
}
