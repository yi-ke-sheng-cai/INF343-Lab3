package main

import (
	"math/rand"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"
	"distrieats/internal/vclock"
)


func (d *Datanode) gossipLoop(min, max time.Duration) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(d.id))))
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

func (d *Datanode) aggregateClock(orders []*pb.Order) *pb.VectorClock {
	agg := &pb.VectorClock{Entries: map[string]int64{}}
	for _, o := range orders {
		agg = vclock.Merge(agg, o.GetClock())
	}
	return agg
}
