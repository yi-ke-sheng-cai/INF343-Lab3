package main

import (
	"sync"
	"time"
)

// SessionEntry asocia un cliente al Datanode donde escribió, con vencimiento.
type SessionEntry struct {
	DatanodeID string
	ExpiresAt  time.Time
}

// sessionStore mantiene la afinidad de sesión client_id -> Datanode con TTL,
// protegida por RWMutex. Garantiza Read Your Writes redirigiendo las lecturas
// al mismo Datanode que procesó la escritura mientras la sesión no expire.
type sessionStore struct {
	mu  sync.RWMutex
	ttl time.Duration
	m   map[string]SessionEntry
}

func newSessionStore(ttl time.Duration) *sessionStore {
	return &sessionStore{ttl: ttl, m: make(map[string]SessionEntry)}
}

// set registra/renueva la afinidad de un cliente con TTL fresco.
func (s *sessionStore) set(clientID, datanodeID string) {
	s.mu.Lock()
	s.m[clientID] = SessionEntry{DatanodeID: datanodeID, ExpiresAt: time.Now().Add(s.ttl)}
	s.mu.Unlock()
}

// get devuelve el Datanode afín si la sesión existe y no expiró (verificación
// lazy: una entrada vencida se trata como ausente).
func (s *sessionStore) get(clientID string) (string, bool) {
	s.mu.RLock()
	e, ok := s.m[clientID]
	s.mu.RUnlock()
	if !ok || time.Now().After(e.ExpiresAt) {
		return "", false
	}
	return e.DatanodeID, true
}

// cleanup expira periódicamente las entradas vencidas para acotar memoria.
func (s *sessionStore) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		s.mu.Lock()
		for k, e := range s.m {
			if now.After(e.ExpiresAt) {
				delete(s.m, k)
			}
		}
		s.mu.Unlock()
	}
}
