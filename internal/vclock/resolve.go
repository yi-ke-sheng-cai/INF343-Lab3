package vclock

import (
	pb "distrieats/proto/pb"
)

const (
	StatusRecibido   = "Recibido"
	StatusPreparando = "Preparando"
	StatusEnCamino   = "En Camino"
	StatusEntregado  = "Entregado"
	StatusCancelado  = "Cancelado"
)


func statusRank(status string) int {
	switch status {
	case StatusRecibido:
		return 1
	case StatusPreparando:
		return 2
	case StatusEnCamino:
		return 3
	case StatusEntregado:
		return 4
	case StatusCancelado:
		return 100
	default:
		return 0
	}
}


type Outcome int

const (
	FirstWrite Outcome = iota
	AppliedDominant
	DiscardedStale
	ConflictResolved
)

func (o Outcome) String() string {
	switch o {
	case FirstWrite:
		return "PrimeraEscritura"
	case AppliedDominant:
		return "AplicadoDominante"
	case DiscardedStale:
		return "DescartadoObsoleto"
	default:
		return "ConflictoResuelto"
	}
}

type Resolution struct {
	Winner  *pb.Order 
	Outcome Outcome
	Applied bool
}


func resolveStatus(current, incoming *pb.Order) *pb.Order {
	rc, ri := statusRank(current.Status), statusRank(incoming.Status)
	switch {
	case ri > rc:
		return incoming
	case ri < rc:
		return current
	case incoming.Timestamp != current.Timestamp:
		if incoming.Timestamp > current.Timestamp {
			return incoming
		}
		return current
	case incoming.OrderId > current.OrderId:
		return incoming
	default:
		return current
	}
}


func Resolve(current, incoming *pb.Order) Resolution {
	if current == nil {
		w := cloneOrder(incoming)
		w.Clock = Clone(incoming.GetClock())
		return Resolution{Winner: w, Outcome: FirstWrite, Applied: true}
	}

	merged := Merge(current.GetClock(), incoming.GetClock())
	rel := Compare(incoming.GetClock(), current.GetClock())

	var winner *pb.Order
	var outcome Outcome
	var applied bool

	switch rel {
	case Dominates:
		winner, outcome, applied = incoming, AppliedDominant, true
	case Dominated, Equal:
		winner, outcome, applied = current, DiscardedStale, false
	default: 
		winner = resolveStatus(current, incoming)
		outcome = ConflictResolved
		applied = winner == incoming
	}

	out := cloneOrder(winner)
	out.Clock = merged 
	return Resolution{Winner: out, Outcome: outcome, Applied: applied}
}

func cloneOrder(o *pb.Order) *pb.Order {
	if o == nil {
		return nil
	}
	return &pb.Order{
		OrderId:    o.OrderId,
		ClientId:   o.ClientId,
		Restaurant: o.Restaurant,
		Status:     o.Status,
		Clock:      Clone(o.GetClock()),
		Timestamp:  o.Timestamp,
	}}
