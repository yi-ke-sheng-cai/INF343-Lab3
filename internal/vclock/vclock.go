// Package vclock implementa los relojes vectoriales y la política de
// resolución de conflictos del sistema DistriEats. Es lógica pura (sin gRPC ni
// estado global) para poder testearla de forma aislada; los Datanodes la
// consumen tanto en UpdateOrder como dentro del ciclo de gossip.
package vclock

import (
	"fmt"
	"sort"

	pb "distrieats/proto/pb"
)

// Relation describe la relación causal entre dos relojes vectoriales A y B.
type Relation int

const (
	// Equal: A y B son idénticos entrada por entrada.
	Equal Relation = iota
	// Dominates: A domina causalmente a B (A >= B en todo, A > B en alguna).
	Dominates
	// Dominated: B domina causalmente a A.
	Dominated
	// Concurrent: ni A domina a B ni B domina a A (conflicto potencial).
	Concurrent
)

func (r Relation) String() string {
	switch r {
	case Equal:
		return "Igual"
	case Dominates:
		return "Domina"
	case Dominated:
		return "Dominado"
	default:
		return "Concurrente"
	}
}

// New crea un reloj vectorial vacío con las entradas de los nodos indicados
// inicializadas en cero.
func New(nodeIDs []string) *pb.VectorClock {
	entries := make(map[string]int64, len(nodeIDs))
	for _, id := range nodeIDs {
		entries[id] = 0
	}
	return &pb.VectorClock{Entries: entries}
}

// entriesOf devuelve el mapa de entradas, tolerando relojes o mapas nil.
func entriesOf(c *pb.VectorClock) map[string]int64 {
	if c == nil || c.Entries == nil {
		return map[string]int64{}
	}
	return c.Entries
}

// Clone copia en profundidad un reloj vectorial (nunca devuelve nil).
func Clone(c *pb.VectorClock) *pb.VectorClock {
	out := make(map[string]int64, len(entriesOf(c)))
	for k, v := range entriesOf(c) {
		out[k] = v
	}
	return &pb.VectorClock{Entries: out}
}

// Increment incrementa en 1 la entrada propia del nodo (crea la clave si falta).
// Se llama cada vez que un Datanode origina o aplica localmente un cambio.
func Increment(c *pb.VectorClock, nodeID string) {
	if c.Entries == nil {
		c.Entries = map[string]int64{}
	}
	c.Entries[nodeID]++
}

// Compare determina la relación causal de A respecto de B considerando la unión
// de todas las claves presentes en ambos relojes (una clave ausente vale 0).
func Compare(a, b *pb.VectorClock) Relation {
	ea, eb := entriesOf(a), entriesOf(b)

	keys := map[string]struct{}{}
	for k := range ea {
		keys[k] = struct{}{}
	}
	for k := range eb {
		keys[k] = struct{}{}
	}

	aGreater, bGreater := false, false
	for k := range keys {
		if ea[k] > eb[k] {
			aGreater = true
		} else if ea[k] < eb[k] {
			bGreater = true
		}
	}

	switch {
	case !aGreater && !bGreater:
		return Equal
	case aGreater && !bGreater:
		return Dominates
	case !aGreater && bGreater:
		return Dominated
	default:
		return Concurrent
	}
}

// Merge devuelve un reloj nuevo con el máximo entrada por entrada (unión). Este
// merge se aplica SIEMPRE al recibir una actualización, independientemente de
// qué estado termine ganando.
func Merge(a, b *pb.VectorClock) *pb.VectorClock {
	ea, eb := entriesOf(a), entriesOf(b)
	out := make(map[string]int64, len(ea)+len(eb))
	for k, v := range ea {
		out[k] = v
	}
	for k, v := range eb {
		if v > out[k] {
			out[k] = v
		}
	}
	return &pb.VectorClock{Entries: out}
}

// String rinde el reloj como "[DN1:3, DN2:2, DN3:2]" con claves ordenadas, para
// logs y para el Reporte.txt (formato exacto pedido en el enunciado).
func String(c *pb.VectorClock) string {
	ent := entriesOf(c)
	keys := make([]string, 0, len(ent))
	for k := range ent {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := "["
	for i, k := range keys {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s:%d", k, ent[k])
	}
	return out + "]"
}
