package vclock

import (
	"testing"

	pb "distrieats/proto/pb"
)

func vc(m map[string]int64) *pb.VectorClock { return &pb.VectorClock{Entries: m} }

func TestCompare(t *testing.T) {
	cases := []struct {
		name string
		a, b map[string]int64
		want Relation
	}{
		{"iguales", map[string]int64{"DN1": 1, "DN2": 2}, map[string]int64{"DN1": 1, "DN2": 2}, Equal},
		{"a domina", map[string]int64{"DN1": 2, "DN2": 2}, map[string]int64{"DN1": 1, "DN2": 2}, Dominates},
		{"a dominado", map[string]int64{"DN1": 1, "DN2": 1}, map[string]int64{"DN1": 1, "DN2": 2}, Dominated},
		{"concurrentes", map[string]int64{"DN1": 2, "DN2": 1}, map[string]int64{"DN1": 1, "DN2": 2}, Concurrent},
		{"claves ausentes cuentan como 0", map[string]int64{"DN1": 1}, map[string]int64{"DN2": 1}, Concurrent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Compare(vc(c.a), vc(c.b)); got != c.want {
				t.Fatalf("Compare = %v, want %v", got, c.want)
			}})}}

func TestMerge(t *testing.T) {
	m := Merge(vc(map[string]int64{"DN1": 3, "DN2": 1}), vc(map[string]int64{"DN1": 2, "DN2": 5, "DN3": 1}))
	want := map[string]int64{"DN1": 3, "DN2": 5, "DN3": 1}
	for k, v := range want {
		if m.Entries[k] != v {
			t.Fatalf("Merge[%s] = %d, want %d", k, m.Entries[k], v)
		}}}

func TestIncrementDoesNotMutateSource(t *testing.T) {
	orig := vc(map[string]int64{"DN1": 1})
	cl := Clone(orig)
	Increment(cl, "DN1")
	if orig.Entries["DN1"] != 1 {
		t.Fatalf("Clone/Increment mutó el original: %d", orig.Entries["DN1"])
	}
	if cl.Entries["DN1"] != 2 {
		t.Fatalf("Increment = %d, want 2", cl.Entries["DN1"])
	}}

func TestResolveFirstWrite(t *testing.T) {
	in := &pb.Order{OrderId: "P1", Status: StatusRecibido, Clock: vc(map[string]int64{"DN1": 1})}
	r := Resolve(nil, in)
	if r.Outcome != FirstWrite || !r.Applied {
		t.Fatalf("primera escritura: outcome=%v applied=%v", r.Outcome, r.Applied)
	}}

func TestResolveDominantApplies(t *testing.T) {
	cur := &pb.Order{OrderId: "P1", Status: StatusRecibido, Clock: vc(map[string]int64{"DN1": 1, "DN2": 0})}
	in := &pb.Order{OrderId: "P1", Status: StatusPreparando, Clock: vc(map[string]int64{"DN1": 2, "DN2": 0})}
	r := Resolve(cur, in)
	if r.Outcome != AppliedDominant || r.Winner.Status != StatusPreparando {
		t.Fatalf("dominante: outcome=%v status=%s", r.Outcome, r.Winner.Status)
	}
	if r.Winner.Clock.Entries["DN1"] != 2 {
		t.Fatalf("merge reloj incorrecto: %v", r.Winner.Clock.Entries)
	}}

func TestResolveStaleDiscarded(t *testing.T) {
	cur := &pb.Order{OrderId: "P1", Status: StatusEnCamino, Clock: vc(map[string]int64{"DN1": 3})}
	in := &pb.Order{OrderId: "P1", Status: StatusRecibido, Clock: vc(map[string]int64{"DN1": 1})}
	r := Resolve(cur, in)
	if r.Outcome != DiscardedStale || r.Applied || r.Winner.Status != StatusEnCamino {
		t.Fatalf("obsoleto: outcome=%v applied=%v status=%s", r.Outcome, r.Applied, r.Winner.Status)
	}}

func TestResolveConcurrentAdvances(t *testing.T) {
	cur := &pb.Order{OrderId: "P1", Status: StatusPreparando, Clock: vc(map[string]int64{"DN1": 2, "DN2": 0})}
	in := &pb.Order{OrderId: "P1", Status: StatusEnCamino, Clock: vc(map[string]int64{"DN1": 0, "DN2": 2})}
	r := Resolve(cur, in)
	if r.Outcome != ConflictResolved || r.Winner.Status != StatusEnCamino {
		t.Fatalf("concurrente: outcome=%v status=%s", r.Outcome, r.Winner.Status)
	}
	if r.Winner.Clock.Entries["DN1"] != 2 || r.Winner.Clock.Entries["DN2"] != 2 {
		t.Fatalf("merge en conflicto: %v", r.Winner.Clock.Entries)
	}}


func TestResolveCancelWinsConcurrent(t *testing.T) {
	cur := &pb.Order{OrderId: "P1", Status: StatusEntregado, Clock: vc(map[string]int64{"DN1": 3, "DN2": 0})}
	in := &pb.Order{OrderId: "P1", Status: StatusCancelado, Clock: vc(map[string]int64{"DN1": 0, "DN2": 3})}
	r := Resolve(cur, in)
	if r.Winner.Status != StatusCancelado {
		t.Fatalf("cancelado debe ganar, got %s", r.Winner.Status)
	}}

func TestResolveCancelNotOverwritten(t *testing.T) {
	cur := &pb.Order{OrderId: "P1", Status: StatusCancelado, Clock: vc(map[string]int64{"DN1": 3, "DN2": 0})}
	in := &pb.Order{OrderId: "P1", Status: StatusEntregado, Clock: vc(map[string]int64{"DN1": 0, "DN2": 3})}
	r := Resolve(cur, in)
	if r.Winner.Status != StatusCancelado {
		t.Fatalf("cancelado no debe ser sobrescrito, got %s", r.Winner.Status)
	}
}

func TestResolveDeterministicOrderIndependent(t *testing.T) {
	a := &pb.Order{OrderId: "P1", Status: StatusPreparando, Timestamp: 10, Clock: vc(map[string]int64{"DN1": 2, "DN2": 0})}
	b := &pb.Order{OrderId: "P1", Status: StatusEnCamino, Timestamp: 20, Clock: vc(map[string]int64{"DN1": 0, "DN2": 2})}
	r1 := Resolve(a, b)
	r2 := Resolve(b, a)
	if r1.Winner.Status != r2.Winner.Status {
		t.Fatalf("no determinista: %s vs %s", r1.Winner.Status, r2.Winner.Status)
	}}
