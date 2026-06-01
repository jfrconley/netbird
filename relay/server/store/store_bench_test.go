package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/netbirdio/netbird/shared/relay/messages"
)

func seedStore(b testing.TB, n int) (*Store, []messages.PeerID) {
	b.Helper()
	s := NewStore()
	ids := make([]messages.PeerID, n)
	for i := 0; i < n; i++ {
		id := messages.HashID(fmt.Sprintf("peer-%d", i))
		ids[i] = id
		s.AddPeer(&MocPeer{id: id})
	}
	return s, ids
}

// BenchmarkStorePeer measures the pure read (lookup) path under parallelism.
func BenchmarkStorePeer(b *testing.B) {
	s, ids := seedStore(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := ids[i%len(ids)]
			if _, ok := s.Peer(id); !ok {
				b.Fatal("peer not found")
			}
			i++
		}
	})
}

// BenchmarkStoreReadMostlyWrite measures many parallel readers with an occasional writer.
func BenchmarkStoreReadMostlyWrite(b *testing.B) {
	s, ids := seedStore(b, 1000)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		churn := messages.HashID("churn-peer")
		p := &MocPeer{id: churn}
		for {
			select {
			case <-stop:
				return
			default:
				s.AddPeer(p)
				s.DeletePeer(p)
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = s.Peer(ids[i%len(ids)])
			i++
		}
	})
	b.StopTimer()
	close(stop)
	wg.Wait()
}

// TestStoreConcurrentReadWrite exercises the store under -race with concurrent readers and churn.
func TestStoreConcurrentReadWrite(t *testing.T) {
	s, ids := seedStore(t, 256)

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)

	// readers
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				_, _ = s.Peer(ids[i%len(ids)])
				_ = s.Peers()
				i++
			}
		}()
	}

	// writers churning add/delete
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := messages.HashID(fmt.Sprintf("churn-%d", w))
			p := &MocPeer{id: id}
			for !stop.Load() {
				s.AddPeer(p)
				s.DeletePeer(p)
			}
		}(w)
	}

	// let them run a bit
	for i := 0; i < 100000; i++ {
		_, _ = s.Peer(ids[i%len(ids)])
	}
	stop.Store(true)
	wg.Wait()
}
