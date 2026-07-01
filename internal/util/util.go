// Package util reúne helpers transversales: resolución de configuración
// (flag > env > default), generación de IDs sin librerías externas y utilidades
// de conexión gRPC. Todo aquí respeta la restricción de "nada hardcodeado".
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

// EnvOr devuelve el valor de la variable de entorno key, o def si está vacía.
// Sirve como default de flags para que la config venga de env vars o flags.
func EnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvDurationOr parsea una duración desde env (ej. "5s"), con default.
func EnvDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// EnvIntOr parsea un entero desde env, con default.
func EnvIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// SplitList divide una lista separada por comas descartando espacios y vacíos.
// Se usa para -nodos "DN1@host:port,DN2@host:port".
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

// Peer representa un Datanode direccionable: su id lógico y su dirección gRPC.
type Peer struct {
	ID   string
	Addr string
}

// ParsePeers interpreta "DN1@10.0.0.2:50061,DN2@10.0.0.3:50062" en una lista de
// Peer. Si una entrada no trae "@id", se autogenera "DNk" por posición.
func ParsePeers(s string) []Peer {
	items := SplitList(s)
	out := make([]Peer, 0, len(items))
	for i, it := range items {
		if idx := strings.Index(it, "@"); idx > 0 {
			out = append(out, Peer{ID: it[:idx], Addr: it[idx+1:]})
		} else {
			out = append(out, Peer{ID: fmt.Sprintf("DN%d", i+1), Addr: it})
		}
	}
	return out
}

// --- Generación de IDs (sin librería uuid) ---

var (
	idMu  sync.Mutex
	idSeq uint64
	idRnd = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// GenID produce un identificador único razonable combinando prefijo, timestamp
// en nanosegundos, una secuencia local y un componente aleatorio. Suficiente
// para request_id de idempotencia sin dependencias externas.
func GenID(prefix string) string {
	idMu.Lock()
	idSeq++
	seq := idSeq
	r := idRnd.Int63()
	idMu.Unlock()
	return fmt.Sprintf("%s-%d-%d-%x", prefix, time.Now().UnixNano(), seq, r)
}

// --- Conexión gRPC ---

// Dial abre una conexión gRPC insegura (sin TLS, entorno de laboratorio) con la
// dirección indicada. No bloquea: los stubs manejan reintentos por RPC.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// CtxTimeout crea un context con timeout para acotar cada RPC saliente.
func CtxTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
