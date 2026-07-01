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

// Gateway es el único punto de entrada de los clientes. Garantiza Read Your
// Writes mediante afinidad de sesión y delega el balanceo al Broker.
type Gateway struct {
	pb.UnimplementedGatewayServiceServer

	log      *log.Logger
	sessions *sessionStore

	brokerAddr   string
	brokerConn   *grpc.ClientConn
	brokerClient pb.BrokerServiceClient

	// Conexiones directas a Datanodes por ID (para lecturas con afinidad).
	dnClients map[string]pb.DatanodeServiceClient

	dial time.Duration

	// Idempotencia: respuestas cacheadas por request_id.
	idemMu  sync.Mutex
	idem    map[string]*pb.CrearPedidoResponse
	idemTTL time.Duration

	// Auditoría RYW para el Reporte.txt (dedup por client_id+order_id).
	rywMu sync.Mutex
	ryw   map[string]*pb.RYWEntry
}

// NewGateway crea el Gateway con conexiones al Broker y a cada Datanode.
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

// CrearPedido: recibe la escritura del cliente, la enruta vía Broker a un
// Datanode, registra la afinidad de sesión y confirma. Idempotente por request_id.
func (g *Gateway) CrearPedido(ctx context.Context, req *pb.CrearPedidoRequest) (*pb.CrearPedidoResponse, error) {
	// Idempotencia: reintento con mismo request_id -> misma respuesta.
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
		// No hubo Datanode que procesara (todos caídos): error de negocio propagado.
		out := &pb.CrearPedidoResponse{Success: false, Message: resp.GetMessage()}
		g.log.Printf("CrearPedido cliente=%s order=%s -> FALLO: %s", req.GetClientId(), req.GetOrderId(), resp.GetMessage())
		return out, nil
	}

	// Registrar afinidad: futuras lecturas de este cliente van a ese Datanode.
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

// ConsultarEstado: con afinidad activa fuerza la lectura al Datanode afín
// (bypass del Broker → RYW); sin afinidad delega el balanceo al Broker.
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
			// El Datanode afín cayó: caer al Broker en vez de fallar.
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

// ObtenerAuditoriaRYW entrega al Broker las validaciones RYW para el Reporte.txt.
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

// registrarRYW guarda una validación RYW (dedup por cliente+pedido).
func (g *Gateway) registrarRYW(clientID, orderID, dnID string) {
	g.rywMu.Lock()
	g.ryw[clientID+"|"+orderID] = &pb.RYWEntry{ClientId: clientID, OrderId: orderID, DatanodeId: dnID}
	g.rywMu.Unlock()
}

// cleanupIdem expira las respuestas idempotentes cacheadas.
func (g *Gateway) cleanupIdem() {
	ticker := time.NewTicker(g.idemTTL)
	defer ticker.Stop()
	for range ticker.C {
		g.idemMu.Lock()
		// Vaciado simple: al vencer el ciclo se limpia todo el caché (los
		// reintentos ocurren en ventanas cortas). Acota memoria sin timestamps.
		g.idem = make(map[string]*pb.CrearPedidoResponse)
		g.idemMu.Unlock()
	}
}
