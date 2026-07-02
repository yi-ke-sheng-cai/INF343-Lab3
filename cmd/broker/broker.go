package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"

	"google.golang.org/grpc"
)

type nodeState struct {
	peer   util.Peer
	conn   *grpc.ClientConn
	client pb.DatanodeServiceClient
	alive  atomic.Bool
}


type Broker struct {
	pb.UnimplementedBrokerServiceServer

	log   *log.Logger
	nodes []*nodeState
	rrIdx uint64 
	dial  time.Duration

	gatewayAddr string
	reportPath  string
	grace       time.Duration
	reported    atomic.Bool 
	stop        chan struct{}
}

func NewBroker(peers []util.Peer, dial, grace time.Duration, gatewayAddr, reportPath string) (*Broker, error) {
	b := &Broker{
		log:         log.New(os.Stdout, "[BROKER] ", log.LstdFlags|log.Lmicroseconds),
		dial:        dial,
		gatewayAddr: gatewayAddr,
		reportPath:  reportPath,
		grace:       grace,
		stop:        make(chan struct{}),
	}
	for _, p := range peers {
		conn, err := util.Dial(p.Addr)
		if err != nil {
			return nil, fmt.Errorf("dial %s (%s): %w", p.ID, p.Addr, err)
		}
		ns := &nodeState{peer: p, conn: conn, client: pb.NewDatanodeServiceClient(conn)}
		ns.alive.Store(true) 
		b.nodes = append(b.nodes, ns)
	}
	return b, nil
}

func (b *Broker) pickLive() *nodeState {
	n := len(b.nodes)
	if n == 0 {
		return nil
	}
	start := atomic.AddUint64(&b.rrIdx, 1)
	for i := 0; i < n; i++ {
		ns := b.nodes[int(start+uint64(i))%n]
		if ns.alive.Load() {
			return ns
		}
	}
	return nil
}


func (b *Broker) routeUpdate(req *pb.UpdateOrderRequest, origen string) (*pb.UpdateOrderResponse, error) {
	for attempt := 0; attempt < len(b.nodes); attempt++ {
		ns := b.pickLive()
		if ns == nil {
			b.log.Printf("%s order=%s -> NO hay Datanodes vivos", origen, req.GetOrder().GetOrderId())
			return &pb.UpdateOrderResponse{Applied: false, Message: "sin Datanodes vivos"}, nil
		}
		ctx, cancel := util.CtxTimeout(b.dial)
		resp, err := ns.client.UpdateOrder(ctx, req)
		cancel()
		if err != nil {
			b.log.Printf("%s: %s no respondió UpdateOrder (%v) -> excluyo y reintento", origen, ns.peer.ID, err)
			ns.alive.Store(false)
			continue
		}
		b.log.Printf("%s order=%s status=%q -> %s (applied=%v estado=%q)",
			origen, req.GetOrder().GetOrderId(), req.GetOrder().GetStatus(), ns.peer.ID, resp.GetApplied(), resp.GetResultingOrder().GetStatus())
		return resp, nil
	}
	return &pb.UpdateOrderResponse{Applied: false, Message: "todos los Datanodes fallaron"}, nil
}


func (b *Broker) EnrutarEscritura(_ context.Context, req *pb.UpdateOrderRequest) (*pb.UpdateOrderResponse, error) {
	return b.routeUpdate(req, "ESCRITURA")
}

func (b *Broker) EmitirEventoLogistico(_ context.Context, req *pb.UpdateOrderRequest) (*pb.UpdateOrderResponse, error) {
	return b.routeUpdate(req, "EVENTO-CSV")
}

func (b *Broker) EnrutarLectura(_ context.Context, req *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	for attempt := 0; attempt < len(b.nodes); attempt++ {
		ns := b.pickLive()
		if ns == nil {
			return &pb.GetOrderResponse{Found: false}, nil
		}
		ctx, cancel := util.CtxTimeout(b.dial)
		resp, err := ns.client.GetOrder(ctx, req)
		cancel()
		if err != nil {
			b.log.Printf("LECTURA: %s no respondió (%v) -> excluyo y reintento", ns.peer.ID, err)
			ns.alive.Store(false)
			continue
		}
		b.log.Printf("LECTURA order=%s -> %s (found=%v)", req.GetOrderId(), ns.peer.ID, resp.GetFound())
		return resp, nil
	}
	return &pb.GetOrderResponse{Found: false}, nil
}


func (b *Broker) SenalarFinEventos(_ context.Context, req *pb.FinEventosRequest) (*pb.FinEventosResponse, error) {
	b.log.Printf("FIN DE EVENTOS recibido (%d eventos). Fase 5: esperando %s de gracia para converger...", req.GetTotalEventos(), b.grace)
	go func() {
		time.Sleep(b.grace)
		if err := b.GenerarReporte(); err != nil {
			b.log.Printf("error generando Reporte.txt: %v", err)
		}
		close(b.stop)
	}()
	return &pb.FinEventosResponse{Ok: true}, nil
}


func (b *Broker) healthCheck(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		for _, ns := range b.nodes {
			ctx, cancel := util.CtxTimeout(b.dial)
			_, err := ns.client.Ping(ctx, &pb.PingRequest{})
			cancel()
			was := ns.alive.Load()
			now := err == nil
			ns.alive.Store(now)
			if was != now {
				if now {
					b.log.Printf("health: %s VOLVIÓ (reincorporado al Round Robin)", ns.peer.ID)
				} else {
					b.log.Printf("health: %s CAÍDO (excluido del Round Robin): %v", ns.peer.ID, err)
				}}}}}
