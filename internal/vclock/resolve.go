package vclock

import (
	pb "distrieats/proto/pb"
)

// Estados de negocio y su orden de avance lógico. Se exportan como constantes
// para evitar strings mágicos repartidos por el código.
const (
	StatusRecibido   = "Recibido"
	StatusPreparando = "Preparando"
	StatusEnCamino   = "En Camino"
	StatusEntregado  = "Entregado"
	StatusCancelado  = "Cancelado"
)

// statusRank asigna un rango al estado según la cadena de avance:
//
//	Recibido < Preparando < En Camino < Entregado
//
// Cancelado recibe prioridad absoluta (mayor que cualquier otro), por lo que
// nunca es sobrescrito y siempre sobrescribe.
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
		// Estado desconocido: rango 0 para no ganar nunca frente a uno válido.
		return 0
	}
}

// Outcome describe qué decidió la resolución al aplicar un Order entrante sobre
// el estado guardado localmente.
type Outcome int

const (
	// FirstWrite: no existía estado previo para ese order_id.
	FirstWrite Outcome = iota
	// AppliedDominant: el entrante domina causalmente y se aplica.
	AppliedDominant
	// DiscardedStale: el guardado domina al entrante; se descarta (evita retroceso).
	DiscardedStale
	// ConflictResolved: eran concurrentes; se aplicó la política determinista.
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

// Resolution es el resultado de aplicar un Order entrante sobre el estado local.
type Resolution struct {
	Winner  *pb.Order // estado que queda persistido (con el reloj ya fusionado)
	Outcome Outcome
	// Applied indica si el CONTENIDO entrante cambió el estado guardado (para
	// el campo applied de UpdateOrderResponse). En un descarte es false.
	Applied bool
}

// resolveStatus decide, entre dos pedidos concurrentes, cuál estado gana según
// la política determinista (más avanzado en la cadena; Cancelado siempre gana).
// Ante empate de rango, desempata por timestamp mayor y luego por order_id para
// garantizar determinismo total y convergencia idéntica en todos los nodos.
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

// Resolve aplica el algoritmo completo de recepción de una actualización:
//
//  1. Si no había estado previo → aplicar directo (FirstWrite).
//  2. Comparar relojes: si el entrante domina → aplicar; si es dominado →
//     descartar el contenido; si son concurrentes → resolver por política de
//     estado.
//  3. El reloj vectorial resultante es SIEMPRE el merge de ambos, sin importar
//     qué contenido gane.
//
// Devuelve una copia del ganador con el reloj ya fusionado; no muta las entradas.
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
		// El estado guardado ya refleja (o supera) al entrante: no retroceder.
		winner, outcome, applied = current, DiscardedStale, false
	default: // Concurrent
		winner = resolveStatus(current, incoming)
		outcome = ConflictResolved
		applied = winner == incoming
	}

	out := cloneOrder(winner)
	out.Clock = merged // el reloj SIEMPRE se fusiona
	return Resolution{Winner: out, Outcome: outcome, Applied: applied}
}

// cloneOrder copia en profundidad un Order (excepto el reloj, que el llamador
// reemplaza por el merge).
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
	}
}
