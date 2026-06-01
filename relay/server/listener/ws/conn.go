package ws

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	log "github.com/sirupsen/logrus"
)

type Conn struct {
	*websocket.Conn
	rAddr *net.TCPAddr

	closed   bool
	closedMu sync.Mutex

	// writeStartedAt holds the unix-nano timestamp of an in-flight write, or 0 when idle.
	// It lets the relay's maintenance loop detect and reap a stuck write without arming a
	// per-write timeout context (which created a runtime timer on every packet).
	writeStartedAt atomic.Int64
}

func NewConn(wsConn *websocket.Conn, rAddr *net.TCPAddr) *Conn {
	return &Conn{
		Conn:  wsConn,
		rAddr: rAddr,
	}
}

func (c *Conn) Read(ctx context.Context, b []byte) (n int, err error) {
	t, r, err := c.Reader(ctx)
	if err != nil {
		return 0, c.ioErrHandling(err)
	}

	if t != websocket.MessageBinary {
		log.Errorf("unexpected message type: %d", t)
		return 0, fmt.Errorf("unexpected message type")
	}

	n, err = r.Read(b)
	if err != nil {
		return 0, c.ioErrHandling(err)
	}
	return n, err
}

// Write writes a binary message with the given payload.
// The caller-supplied context (a long-lived, deadline-less peer context) is passed straight to the
// websocket library, so no per-write runtime timer is created. A write that blocks (slow/stuck peer)
// is reaped by the relay maintenance loop via WriteStalled, which closes the connection and unblocks
// this call with net.ErrClosed.
func (c *Conn) Write(ctx context.Context, b []byte) (int, error) {
	c.writeStartedAt.Store(time.Now().UnixNano())
	defer c.writeStartedAt.Store(0)

	err := c.Conn.Write(ctx, websocket.MessageBinary, b)
	return len(b), err
}

// WriteStalled reports whether a write has been in flight longer than timeout.
func (c *Conn) WriteStalled(nowUnixNano int64, timeout time.Duration) bool {
	started := c.writeStartedAt.Load()
	return started != 0 && nowUnixNano-started > int64(timeout)
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.rAddr
}

func (c *Conn) Close() error {
	c.closedMu.Lock()
	c.closed = true
	c.closedMu.Unlock()
	return c.CloseNow()
}

func (c *Conn) isClosed() bool {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	return c.closed
}

func (c *Conn) ioErrHandling(err error) error {
	if c.isClosed() {
		return net.ErrClosed
	}

	var wErr *websocket.CloseError
	if !errors.As(err, &wErr) {
		return err
	}
	if wErr.Code == websocket.StatusNormalClosure {
		return net.ErrClosed
	}
	return err
}
