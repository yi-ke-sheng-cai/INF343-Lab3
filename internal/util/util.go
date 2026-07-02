package util

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


func EnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func EnvDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func EnvIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}


func SplitList(s string) []string {
	raw := strings.Split(s, ",")
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if t := strings.TrimSpace(r); t != "" {
			out = append(out, t)
		}
	}
	return out
}

type Peer struct {
	ID   string
	Addr string
}

func ParsePeers(s string) []Peer {
	items := SplitList(s)
	out := make([]Peer, 0, len(items))
	for i, it := range items {
		if idx := strings.Index(it, "@"); idx > 0 {
			out = append(out, Peer{ID: it[:idx], Addr: it[idx+1:]})
		} 
		else {
			out = append(out, Peer{ID: fmt.Sprintf("DN%d", i+1), Addr: it})
		}}
	return out
}


var (
	idMu  sync.Mutex
	idSeq uint64
	idRnd = rand.New(rand.NewSource(time.Now().UnixNano()))
)


func GenID(prefix string) string {
	idMu.Lock()
	idSeq++
	seq := idSeq
	r := idRnd.Int63()
	idMu.Unlock()
	return fmt.Sprintf("%s-%d-%d-%x", prefix, time.Now().UnixNano(), seq, r)
}


func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func CtxTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
