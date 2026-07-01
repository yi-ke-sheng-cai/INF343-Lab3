// Command gateway es el único punto de entrada de los clientes. Garantiza Read
// Your Writes mediante afinidad de sesión con TTL y delega el balanceo al Broker.
//
// Configuración (flag > env var > default):
//
//	-puerto     puerto gRPC de escucha                  env GW_PUERTO
//	-broker     dirección del Broker                    env GW_BROKER
//	-nodos      Datanodes 'DN1@host:port,...'           env GW_NODOS
//	-ttl        TTL de afinidad de sesión               env GW_TTL
//	-idem-ttl   TTL del caché de idempotencia           env GW_IDEM_TTL
//	-rpc-timeout timeout de RPC saliente                env GW_RPC_TIMEOUT
package main

import (
	"flag"
	"log"
	"net"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"

	"google.golang.org/grpc"
)

func main() {
	puerto := flag.String("puerto", util.EnvOr("GW_PUERTO", "50040"), "puerto gRPC de escucha")
	broker := flag.String("broker", util.EnvOr("GW_BROKER", "localhost:50051"), "dirección del Broker")
	nodos := flag.String("nodos", util.EnvOr("GW_NODOS", ""), "Datanodes 'DN1@host:port,...'")
	ttl := flag.Duration("ttl", util.EnvDurationOr("GW_TTL", 60*time.Second), "TTL de afinidad de sesión")
	idemTTL := flag.Duration("idem-ttl", util.EnvDurationOr("GW_IDEM_TTL", 120*time.Second), "TTL del caché de idempotencia")
	rpcTimeout := flag.Duration("rpc-timeout", util.EnvDurationOr("GW_RPC_TIMEOUT", 3*time.Second), "timeout de RPC saliente")
	flag.Parse()

	datanodes := util.ParsePeers(*nodos)
	if len(datanodes) == 0 {
		log.Fatal("[GATEWAY] no se configuraron Datanodes (-nodos)")
	}

	g, err := NewGateway(*broker, datanodes, *ttl, *idemTTL, *rpcTimeout)
	if err != nil {
		log.Fatalf("[GATEWAY] init: %v", err)
	}

	lis, err := net.Listen("tcp", ":"+*puerto)
	if err != nil {
		log.Fatalf("[GATEWAY] no pude escuchar en :%s: %v", *puerto, err)
	}

	srv := grpc.NewServer()
	pb.RegisterGatewayServiceServer(srv, g)

	go g.sessions.cleanup(*ttl)
	go g.cleanupIdem()

	g.log.Printf("escuchando gRPC en :%s | broker=%s | Datanodes=%d | TTL afinidad=%s", *puerto, *broker, len(datanodes), *ttl)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("[GATEWAY] gRPC terminó: %v", err)
	}
}
