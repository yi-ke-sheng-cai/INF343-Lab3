package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"
	"distrieats/internal/vclock"
)


type Datanode struct {
	pb.UnimplementedDatanodeServiceServer

	id        string
	log       *log.Logger
	finalPath string
	shutdown  chan struct{}

	peers  []util.Peer   
	dial   time.Duration 
	reqTTL time.Duration 

	mu      sync.RWMutex
	orders  map[string]*pb.Order 
	seenReq map[string]time.Time 
}


func NewDatanode(id string, peers []util.Peer, dial, reqTTL time.Duration, finalPath string) *Datanode {
	return &Datanode{
		id:        id,
		log:       log.New(os.Stdout, fmt.Sprintf("[DATANODE-%s] ", id), log.LstdFlags|log.Lmicroseconds),
		finalPath: finalPath,
		shutdown:  make(chan struct{}),
		peers:     peers,
		dial:      dial,
		reqTTL:    reqTTL,
		orders:    make(map[string]*pb.Order),
		seenReq:   make(map[string]time.Time),
	}
}


func (d *Datanode) UpdateOrder(_ context.Context, req *pb.UpdateOrderRequest) (*pb.UpdateOrderResponse, error) {
	in := req.GetOrder()
	if in == nil || in.GetOrderId() == "" {
		return &pb.UpdateOrderResponse{Applied: false, DatanodeId: d.id, Message: "order inválida"}, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if id := req.GetRequestId(); id != "" {
		if _, seen := d.seenReq[id]; seen {
			cur := d.orders[in.GetOrderId()]
			d.log.Printf("UpdateOrder DUPLICADO request_id=%s order=%s -> ignorado (idempotencia)", id, in.GetOrderId())
			return &pb.UpdateOrderResponse{Applied: false, ResultingOrder: cur, DatanodeId: d.id, Message: "duplicado"}, nil
		}
		d.seenReq[id] = time.Now()
	}

	cur := d.orders[in.GetOrderId()]

	candidate := &pb.Order{
		OrderId:    in.GetOrderId(),
		ClientId:   in.GetClientId(),
		Restaurant: in.GetRestaurant(),
		Status:     in.GetStatus(),
		Timestamp:  time.Now().UnixNano(),
		Clock:      vclock.Clone(clockOf(cur)),
	}
	vclock.Increment(candidate.Clock, d.id)

	res := vclock.Resolve(cur, candidate)
	d.orders[in.GetOrderId()] = res.Winner

	d.log.Printf("UpdateOrder order=%s status_in=%q resultado=%s -> estado=%q reloj=%s",
		in.GetOrderId(), in.GetStatus(), res.Outcome, res.Winner.Status, vclock.String(res.Winner.Clock))

	return &pb.UpdateOrderResponse{
		Applied:        res.Applied,
		ResultingOrder: res.Winner,
		DatanodeId:     d.id,
	}, nil
}

func (d *Datanode) GetOrder(_ context.Context, req *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	o, ok := d.orders[req.GetOrderId()]
	if !ok {
		d.log.Printf("GetOrder order=%s -> NO ENCONTRADO", req.GetOrderId())
		return &pb.GetOrderResponse{Found: false, DatanodeId: d.id}, nil
	}
	d.log.Printf("GetOrder order=%s -> estado=%q reloj=%s", req.GetOrderId(), o.Status, vclock.String(o.Clock))
	return &pb.GetOrderResponse{Found: true, Order: o, DatanodeId: d.id}, nil
}


func (d *Datanode) GossipSync(_ context.Context, req *pb.GossipSyncRequest) (*pb.GossipSyncResponse, error) {
	d.mu.Lock()
	applied := d.mergeReplicated(req.GetOrders(), "gossip<-"+req.GetSenderId())
	out := d.snapshotLocked()
	d.mu.Unlock()

	if applied > 0 {
		d.log.Printf("GossipSync de %s: %d órdenes fusionadas, devuelvo %d", req.GetSenderId(), applied, len(out))
	}
	return &pb.GossipSyncResponse{Orders: out, ReceiverId: d.id}, nil
}

func (d *Datanode) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{Alive: true, NodeId: d.id}, nil
}

func (d *Datanode) Shutdown(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	d.log.Printf("Shutdown recibido del Broker, escribiendo estado final...")
	if p := d.finalPath; p != "" {
		if err := d.WriteFinalState(p); err != nil {
			d.log.Printf("error al volcar estado final por shutdown: %v", err)
		}
	}
	close(d.shutdown)
	return &pb.PingResponse{Alive: true, NodeId: d.id}, nil
}

func (d *Datanode) Snapshot(_ context.Context, _ *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return &pb.SnapshotResponse{Orders: d.snapshotLocked(), NodeId: d.id}, nil
}


func (d *Datanode) mergeReplicated(orders []*pb.Order, source string) int {
	changed := 0
	for _, in := range orders {
		if in == nil || in.GetOrderId() == "" {
			continue
		}
		cur := d.orders[in.GetOrderId()]
		res := vclock.Resolve(cur, in)
		if res.Outcome == vclock.ConflictResolved {
			d.log.Printf("CONFLICTO [%s] order=%s | local=%q %s vs entrante=%q %s -> gana %q reloj=%s",
				source, in.GetOrderId(), statusOf(cur), vclock.String(clockOf(cur)),
				in.GetStatus(), vclock.String(in.GetClock()), res.Winner.Status, vclock.String(res.Winner.Clock))
		}
		if res.Outcome != vclock.DiscardedStale {
			if cur == nil || statusOf(cur) != res.Winner.Status || vclock.Compare(clockOf(cur), res.Winner.Clock) != vclock.Equal {
				changed++
			}
			d.orders[in.GetOrderId()] = res.Winner
		}
	}
	return changed
}


func (d *Datanode) snapshotLocked() []*pb.Order {
	out := make([]*pb.Order, 0, len(d.orders))
	for _, o := range d.orders {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OrderId < out[j].OrderId })
	return out
}

func (d *Datanode) cleanupSeen() {
	ticker := time.NewTicker(d.reqTTL)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		d.mu.Lock()
		for id, t := range d.seenReq {
			if now.Sub(t) > d.reqTTL {
				delete(d.seenReq, id)
			}
		}
		d.mu.Unlock()
	}
}


func (d *Datanode) WriteFinalState(path string) error {
	d.mu.RLock()
	orders := d.snapshotLocked()
	d.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "=== ESTADO FINAL DATANODE %s ===\n", d.id)
	for _, o := range orders {
		fmt.Fprintf(f, "Pedido ID: %s | Estado Final: %s | Reloj Vectorial: %s\n",
			o.OrderId, o.Status, vclock.String(o.Clock))
	}
	d.log.Printf("Estado final volcado a %s (%d pedidos)", path, len(orders))
	return nil
}



func clockOf(o *pb.Order) *pb.VectorClock {
	if o == nil {
		return nil
	}
	return o.Clock
}

func statusOf(o *pb.Order) string {
	if o == nil {
		return "<nuevo>"
	}
	return o.Status
}
