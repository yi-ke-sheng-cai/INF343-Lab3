package main

import (
	"sync"
	"time"
)

type SessionEntry struct {
	DatanodeID string
	ExpiresAt  time.Time
}

type sessionStore struct {
	mu  sync.RWMutex
	ttl time.Duration
	m   map[string]SessionEntry
}

func newSessionStore(ttl time.Duration) *sessionStore {
	return &sessionStore{ttl: ttl, m: make(map[string]SessionEntry)}
}

func (s *sessionStore) set(clientID, datanodeID string) {
	s.mu.Lock()
	s.m[clientID] = SessionEntry{DatanodeID: datanodeID, ExpiresAt: time.Now().Add(s.ttl)}
	s.mu.Unlock()
}

func (s *sessionStore) get(clientID string) (string, bool) {
	s.mu.RLock()
	e, ok := s.m[clientID]
	s.mu.RUnlock()
	if !ok || time.Now().After(e.ExpiresAt) {
		return "", false
	}
	return e.DatanodeID, true
}

func (s *sessionStore) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		s.mu.Lock()
		for k, e := range s.m {
			if now.After(e.ExpiresAt) {
				delete(s.m, k)
			}}
		s.mu.Unlock()
	}}
