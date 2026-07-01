package main

import (
	"math/rand"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"
	"distrieats/internal/vclock"
)

// gossipLoop corre en background: cada intervalo (jitter entre min y max) elige
// un peer vivo al azar y se sincroniza con él. Tolera peers caídos sin crashear.
func (d *Datanode) gossipLoop(min, max time.Duration) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(d.id))))
	// Arranque: gossip inmediato para pedir estado histórico (recuperación).
	d.gossipOnce(rnd)
	for {
		wait := min
		if max > min {
			wait += time.Duration(rnd.Int63n(int64(max - min)))
		}
		time.Sleep(wait)
		d.gossipOnce(rnd)
	}
}

// gossipOnce ejecuta un ciclo: escoge un peer aleatorio, le envía el snapshot
// local y fusiona lo que devuelve.
func (d *Datanode) gossipOnce(rnd *rand.Rand) {
	if len(d.peers) == 0 {
		return
	}
	peer := d.peers[rnd.Intn(len(d.peers))]

	d.mu.RLock()
	snap := d.snapshotLocked()
	d.mu.RUnlock()

	conn, err := util.Dial(peer.Addr)
	if err != nil {
		d.log.Printf("gossip: no pude conectar a %s (%s): %v", peer.ID, peer.Addr, err)
		return
	}
	defer conn.Close()

	ctx, cancel := util.CtxTimeout(d.dial)
	defer cancel()

	resp, err := pb.NewDatanodeServiceClient(conn).GossipSync(ctx, &pb.GossipSyncRequest{
		Orders:      snap,
		SenderId:    d.id,
		SenderClock: d.aggregateClock(snap),
	})
	if err != nil {
		// Peer caído: loggear y continuar (no crashear).
		d.log.Printf("gossip: %s no respondió (%s): %v", peer.ID, peer.Addr, err)
		return
	}

	d.mu.Lock()
	applied := d.mergeReplicated(resp.GetOrders(), "gossip-resp<-"+resp.GetReceiverId())
	d.mu.Unlock()
	if applied > 0 {
		d.log.Printf("gossip: sincronizado con %s, %d órdenes actualizadas desde su respuesta", peer.ID, applied)
	}
}

// aggregateClock construye un reloj informativo agregando (máximo) los relojes
// de todas las órdenes locales. Se envía como sender_clock (uso de log/depuración).
func (d *Datanode) aggregateClock(orders []*pb.Order) *pb.VectorClock {
	agg := &pb.VectorClock{Entries: map[string]int64{}}
	for _, o := range orders {
		agg = vclock.Merge(agg, o.GetClock())
	}
	return agg
}
