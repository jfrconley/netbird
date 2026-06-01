package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/netbirdio/netbird/client/iface"
	"github.com/netbirdio/netbird/relay/server"
	"github.com/netbirdio/netbird/shared/relay/auth/allow"
	"github.com/netbirdio/netbird/shared/relay/client"
)

// BenchmarkRelaySingleFlow measures single-flow throughput through the relay: one sender
// pipelines chunks to one receiver which drains them. This is the headline metric from
// issue #6021. Run with -cpuprofile to inspect the futex/nanosleep share.
func BenchmarkRelaySingleFlow(b *testing.B) {
	ctx := context.Background()
	const port = 36000
	serverAddress := fmt.Sprintf("127.0.0.1:%d", port)
	serverConnURL := fmt.Sprintf("rel://%s", serverAddress)

	srv, err := server.NewServer(server.Config{
		ExposedAddress: serverConnURL,
		TLSSupport:     false,
		AuthValidator:  &allow.Auth{},
	})
	if err != nil {
		b.Fatalf("failed to create server: %s", err)
	}
	errChan := make(chan error, 1)
	go func() {
		if err := srv.Listen(server.ListenerConfig{Address: serverAddress}); err != nil {
			errChan <- err
		}
	}()
	defer func() { _ = srv.Shutdown(ctx) }()
	if err := waitForServerToStart(errChan); err != nil {
		b.Fatalf("failed to start server: %s", err)
	}

	sender := client.NewClient(serverConnURL, hmacTokenStore, "bench-sender", iface.DefaultMTU)
	if err := sender.Connect(ctx); err != nil {
		b.Fatalf("sender connect: %s", err)
	}
	receiver := client.NewClient(serverConnURL, hmacTokenStore, "bench-receiver", iface.DefaultMTU)
	if err := receiver.Connect(ctx); err != nil {
		b.Fatalf("receiver connect: %s", err)
	}

	sConn, err := sender.OpenConn(ctx, "bench-receiver")
	if err != nil {
		b.Fatalf("sender open conn: %s", err)
	}
	rConn, err := receiver.OpenConn(ctx, "bench-sender")
	if err != nil {
		b.Fatalf("receiver open conn: %s", err)
	}

	chunk := make([]byte, 8192)
	b.SetBytes(int64(len(chunk)))
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rbuf := make([]byte, 32*1024)
		remaining := int64(b.N) * int64(len(chunk))
		for remaining > 0 {
			n, err := rConn.Read(rbuf)
			if err != nil {
				return
			}
			remaining -= int64(n)
		}
	}()

	for i := 0; i < b.N; i++ {
		if _, err := sConn.Write(chunk); err != nil {
			b.Fatalf("write: %s", err)
		}
	}
	wg.Wait()

	b.StopTimer()
	_ = sConn.Close()
	_ = rConn.Close()
}
