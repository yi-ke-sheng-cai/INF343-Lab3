package vclock

import (
	"fmt"
	"sort"

	pb "distrieats/proto/pb"
)

type Relation int

const (
	Equal Relation = iota
	Dominates
	Dominated
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
	}}

func New(nodeIDs []string) *pb.VectorClock {
	entries := make(map[string]int64, len(nodeIDs))
	for _, id := range nodeIDs {
		entries[id] = 0
	}
	return &pb.VectorClock{Entries: entries}
}

func entriesOf(c *pb.VectorClock) map[string]int64 {
	if c == nil || c.Entries == nil {
		return map[string]int64{}
	}
	return c.Entries
}

func Clone(c *pb.VectorClock) *pb.VectorClock {
	out := make(map[string]int64, len(entriesOf(c)))
	for k, v := range entriesOf(c) {
		out[k] = v
	}
	return &pb.VectorClock{Entries: out}
}


func Increment(c *pb.VectorClock, nodeID string) {
	if c.Entries == nil {
		c.Entries = map[string]int64{}
	}
	c.Entries[nodeID]++
}

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
		} 
		else if ea[k] < eb[k] {
			bGreater = true
		}}

	switch {
	case !aGreater && !bGreater:
		return Equal
	case aGreater && !bGreater:
		return Dominates
	case !aGreater && bGreater:
		return Dominated
	default:
		return Concurrent
	}}


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
