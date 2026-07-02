package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"

	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", util.EnvOr("DN_ID", "DN1"), "ID lógico del Datanode")
	puerto := flag.String("puerto", util.EnvOr("DN_PUERTO", "50061"), "puerto gRPC de escucha")
	peers := flag.String("peers", util.EnvOr("DN_PEERS", ""), "peers 'DN2@host:port,DN3@host:port'")
	gossipMin := flag.Duration("gossip-min", util.EnvDurationOr("DN_GOSSIP_MIN", 3*time.Second), "intervalo mínimo de gossip")
	gossipMax := flag.Duration("gossip-max", util.EnvDurationOr("DN_GOSSIP_MAX", 7*time.Second), "intervalo máximo de gossip")
	rpcTimeout := flag.Duration("rpc-timeout", util.EnvDurationOr("DN_RPC_TIMEOUT", 3*time.Second), "timeout de RPC saliente")
	reqTTL := flag.Duration("req-ttl", util.EnvDurationOr("DN_REQ_TTL", 120*time.Second), "TTL de request_id procesados")
	finalLog := flag.String("final-log", util.EnvOr("DN_FINAL_LOG", ""), "ruta del volcado de estado final")
	flag.Parse()

	finalPath := *finalLog
	if finalPath == "" {
		finalPath = "estado_final_" + *id + ".log"
	}

	dn := NewDatanode(*id, util.ParsePeers(*peers), *rpcTimeout, *reqTTL, finalPath)

	lis, err := net.Listen("tcp", ":"+*puerto)
	if err != nil {
		log.Fatalf("[DATANODE-%s] no pude escuchar en :%s: %v", *id, *puerto, err)
	}

	srv := grpc.NewServer()
	pb.RegisterDatanodeServiceServer(srv, dn)

	go dn.gossipLoop(*gossipMin, *gossipMax)
	go dn.cleanupSeen()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-dn.shutdown:
			dn.log.Printf("Shutdown iniciado, deteniendo servidor gRPC...")
		case <-sig:
			dn.log.Printf("Señal recibida, volcando estado final...")
			if err := dn.WriteFinalState(finalPath); err != nil {
				dn.log.Printf("error al volcar estado final: %v", err)
			}
		}
		srv.GracefulStop()
		os.Exit(0)
	}()

	dn.log.Printf("escuchando gRPC en :%s | peers=%d | gossip=[%s,%s]", *puerto, len(dn.peers), *gossipMin, *gossipMax)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("[DATANODE-%s] gRPC terminó: %v", *id, err)
	}
}
