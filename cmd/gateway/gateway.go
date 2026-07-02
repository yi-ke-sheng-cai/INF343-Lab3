package main

import (
	"context"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"
	"distrieats/internal/vclock"

	"google.golang.org/grpc"
)


type Gateway struct {
	pb.UnimplementedGatewayServiceServer

	log      *log.Logger
	sessions *sessionStore

	brokerAddr   string
	brokerConn   *grpc.ClientConn
	brokerClient pb.BrokerServiceClient

	dnClients map[string]pb.DatanodeServiceClient

	dial time.Duration

	idemMu  sync.Mutex
	idem    map[string]*pb.CrearPedidoResponse
	idemTTL time.Duration

	rywMu sync.Mutex
	ryw   map[string]*pb.RYWEntry

	stop chan struct{}
}

func NewGateway(brokerAddr string, datanodes []util.Peer, ttl, idemTTL, dial time.Duration) (*Gateway, error) {
	g := &Gateway{
		log:        log.New(os.Stdout, "[GATEWAY] ", log.LstdFlags|log.Lmicroseconds),
		sessions:   newSessionStore(ttl),
		brokerAddr: brokerAddr,
		dnClients:  make(map[string]pb.DatanodeServiceClient),
		dial:       dial,
		idem:       make(map[string]*pb.CrearPedidoResponse),
		idemTTL:    idemTTL,
		ryw:        make(map[string]*pb.RYWEntry),
		stop:       make(chan struct{}),
	}

	bc, err := util.Dial(brokerAddr)
	if err != nil {
		return nil, err
	}
	g.brokerConn = bc
	g.brokerClient = pb.NewBrokerServiceClient(bc)

	for _, p := range datanodes {
		conn, err := util.Dial(p.Addr)
		if err != nil {
			return nil, err
		}
		g.dnClients[p.ID] = pb.NewDatanodeServiceClient(conn)
	}
	return g, nil
}


func (g *Gateway) CrearPedido(ctx context.Context, req *pb.CrearPedidoRequest) (*pb.CrearPedidoResponse, error) {
	if req.GetRequestId() != "" {
		g.idemMu.Lock()
		if cached, ok := g.idem[req.GetRequestId()]; ok {
			g.idemMu.Unlock()
			g.log.Printf("CrearPedido DUPLICADO request_id=%s -> respuesta cacheada", req.GetRequestId())
			return cached, nil
		}
		g.idemMu.Unlock()
	}

	order := &pb.Order{
		OrderId:    req.GetOrderId(),
		ClientId:   req.GetClientId(),
		Restaurant: req.GetRestaurant(),
		Status:     vclock.StatusRecibido,
		Timestamp:  time.Now().UnixNano(),
	}

	rctx, cancel := util.CtxTimeout(g.dial)
	defer cancel()
	resp, err := g.brokerClient.EnrutarEscritura(rctx, &pb.UpdateOrderRequest{Order: order, RequestId: req.GetRequestId()})
	if err != nil {
		g.log.Printf("CrearPedido cliente=%s order=%s -> ERROR de Broker: %v", req.GetClientId(), req.GetOrderId(), err)
		return &pb.CrearPedidoResponse{Success: false, Message: "broker no disponible: " + err.Error()}, nil
	}
	if resp.GetDatanodeId() == "" {
		out := &pb.CrearPedidoResponse{Success: false, Message: resp.GetMessage()}
		g.log.Printf("CrearPedido cliente=%s order=%s -> FALLO: %s", req.GetClientId(), req.GetOrderId(), resp.GetMessage())
		return out, nil
	}

	g.sessions.set(req.GetClientId(), resp.GetDatanodeId())
	g.log.Printf("CrearPedido cliente=%s order=%s -> %s | afinidad registrada (TTL %s)",
		req.GetClientId(), req.GetOrderId(), resp.GetDatanodeId(), g.sessions.ttl)

	out := &pb.CrearPedidoResponse{
		Success:    true,
		Message:    "pedido creado",
		Order:      resp.GetResultingOrder(),
		DatanodeId: resp.GetDatanodeId(),
	}

	if req.GetRequestId() != "" {
		g.idemMu.Lock()
		g.idem[req.GetRequestId()] = out
		g.idemMu.Unlock()
	}
	return out, nil
}


func (g *Gateway) ConsultarEstado(ctx context.Context, req *pb.ConsultarEstadoRequest) (*pb.ConsultarEstadoResponse, error) {
	if dnID, ok := g.sessions.get(req.GetClientId()); ok {
		if client, exists := g.dnClients[dnID]; exists {
			rctx, cancel := util.CtxTimeout(g.dial)
			resp, err := client.GetOrder(rctx, &pb.GetOrderRequest{OrderId: req.GetOrderId()})
			cancel()
			if err == nil {
				g.log.Printf("ConsultarEstado cliente=%s order=%s -> AFINIDAD %s (found=%v) [RYW]",
					req.GetClientId(), req.GetOrderId(), dnID, resp.GetFound())
				if resp.GetFound() {
					g.registrarRYW(req.GetClientId(), req.GetOrderId(), dnID)
				}
				return &pb.ConsultarEstadoResponse{Found: resp.GetFound(), Order: resp.GetOrder(), DatanodeId: dnID, FromAffinity: true}, nil
			}
			g.log.Printf("ConsultarEstado cliente=%s: Datanode afín %s no responde (%v) -> fallback a Broker", req.GetClientId(), dnID, err)
		}
	}

	rctx, cancel := util.CtxTimeout(g.dial)
	defer cancel()
	resp, err := g.brokerClient.EnrutarLectura(rctx, &pb.GetOrderRequest{OrderId: req.GetOrderId()})
	if err != nil {
		g.log.Printf("ConsultarEstado cliente=%s order=%s -> ERROR Broker: %v", req.GetClientId(), req.GetOrderId(), err)
		return &pb.ConsultarEstadoResponse{Found: false}, nil
	}
	g.log.Printf("ConsultarEstado cliente=%s order=%s -> BROKER/RoundRobin %s (found=%v)",
		req.GetClientId(), req.GetOrderId(), resp.GetDatanodeId(), resp.GetFound())
	return &pb.ConsultarEstadoResponse{Found: resp.GetFound(), Order: resp.GetOrder(), DatanodeId: resp.GetDatanodeId(), FromAffinity: false}, nil
}

func (g *Gateway) ObtenerAuditoriaRYW(_ context.Context, _ *pb.AuditoriaRYWRequest) (*pb.AuditoriaRYWResponse, error) {
	g.rywMu.Lock()
	defer g.rywMu.Unlock()
	out := make([]*pb.RYWEntry, 0, len(g.ryw))
	for _, e := range g.ryw {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetClientId() != out[j].GetClientId() {
			return out[i].GetClientId() < out[j].GetClientId()
		}
		return out[i].GetOrderId() < out[j].GetOrderId()
	})
	return &pb.AuditoriaRYWResponse{Entries: out}, nil
}

func (g *Gateway) registrarRYW(clientID, orderID, dnID string) {
	g.rywMu.Lock()
	g.ryw[clientID+"|"+orderID] = &pb.RYWEntry{ClientId: clientID, OrderId: orderID, DatanodeId: dnID}
	g.rywMu.Unlock()
}

func (g *Gateway) Shutdown(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	g.log.Printf("Shutdown recibido, cerrando...")
	close(g.stop)
	return &pb.PingResponse{Alive: true, NodeId: "gateway"}, nil
}

func (g *Gateway) cleanupIdem() {
	ticker := time.NewTicker(g.idemTTL)
	defer ticker.Stop()
	for range ticker.C {
		g.idemMu.Lock()
		g.idem = make(map[string]*pb.CrearPedidoResponse)
		g.idemMu.Unlock()
	}}
