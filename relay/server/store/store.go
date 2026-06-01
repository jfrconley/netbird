package store

import (
	"sync"
	"sync/atomic"

	"github.com/netbirdio/netbird/shared/relay/messages"
)

type IPeer interface {
	Close()
	ID() messages.PeerID
}

type peerMap = map[messages.PeerID]IPeer

// Store is a thread-safe store of peers.
// It is used to store the peers that are connected to the relay server.
//
// The read path (Peer/Peers/GetOnlinePeers...) is lock-free: it does a single atomic load of an
// immutable map. Writers (AddPeer/DeletePeer) are serialized by writeMu and publish a fresh copy of
// the map (copy-on-write). Reads dominate by orders of magnitude (one lookup per forwarded packet)
// while writes happen only on connect/disconnect, so this trades cheap rare writes for a contention-
// free hot path.
type Store struct {
	peers   atomic.Pointer[peerMap]
	writeMu sync.Mutex
}

// NewStore creates a new Store instance
func NewStore() *Store {
	s := &Store{}
	m := make(peerMap)
	s.peers.Store(&m)
	return s
}

func (s *Store) load() peerMap {
	return *s.peers.Load()
}

// AddPeer adds a peer to the store.
// If the peer already exists, it will be replaced and the old peer will be closed.
// Returns true if the peer was replaced, false if it was added for the first time.
func (s *Store) AddPeer(peer IPeer) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	old := s.load()
	nm := make(peerMap, len(old)+1)
	for k, v := range old {
		nm[k] = v
	}
	existing, ok := nm[peer.ID()]
	nm[peer.ID()] = peer
	s.peers.Store(&nm)

	// Close the replaced peer only after publishing the new map, so concurrent readers never
	// route a packet to a peer that has already been closed but is still present in the map.
	if ok {
		existing.Close()
	}
	return ok
}

// DeletePeer deletes a peer from the store.
// It only deletes the peer if the stored pointer is identical to the given one.
func (s *Store) DeletePeer(peer IPeer) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	old := s.load()
	cur, ok := old[peer.ID()]
	if !ok || cur != peer {
		return false
	}

	nm := make(peerMap, len(old))
	for k, v := range old {
		if k == peer.ID() {
			continue
		}
		nm[k] = v
	}
	s.peers.Store(&nm)
	return true
}

// Peer returns a peer by its ID
func (s *Store) Peer(id messages.PeerID) (IPeer, bool) {
	p, ok := s.load()[id]
	return p, ok
}

// Peers returns all the peers in the store
func (s *Store) Peers() []IPeer {
	m := s.load()
	peers := make([]IPeer, 0, len(m))
	for _, p := range m {
		peers = append(peers, p)
	}
	return peers
}

func (s *Store) GetOnlinePeersAndRegisterInterest(peerIDs []messages.PeerID, listener *Listener) []messages.PeerID {
	listener.AddInterestedPeers(peerIDs)

	m := s.load()
	onlinePeers := make([]messages.PeerID, 0, len(peerIDs))

	// Check for currently online peers
	for _, id := range peerIDs {
		if _, ok := m[id]; ok {
			onlinePeers = append(onlinePeers, id)
		}
	}

	return onlinePeers
}
