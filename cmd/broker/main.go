
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
	puerto := flag.String("puerto", util.EnvOr("BROKER_PUERTO", "50051"), "puerto gRPC de escucha")
	nodos := flag.String("nodos", util.EnvOr("BROKER_NODOS", ""), "Datanodes 'DN1@host:port,...'")
	gateway := flag.String("gateway", util.EnvOr("BROKER_GATEWAY", ""), "dirección del Gateway (para auditoría RYW)")
	health := flag.Duration("health", util.EnvDurationOr("BROKER_HEALTH", 3*time.Second), "intervalo de health check")
	rpcTimeout := flag.Duration("rpc-timeout", util.EnvDurationOr("BROKER_RPC_TIMEOUT", 3*time.Second), "timeout de RPC saliente")
	grace := flag.Duration("grace", util.EnvDurationOr("BROKER_GRACE", 15*time.Second), "ventana de gracia antes del Reporte.txt")
	reporte := flag.String("reporte", util.EnvOr("BROKER_REPORTE", "Reporte.txt"), "ruta del Reporte.txt")
	flag.Parse()

	peers := util.ParsePeers(*nodos)
	if len(peers) == 0 {
		log.Fatal("[BROKER] no se configuraron Datanodes (-nodos)")
	}

	b, err := NewBroker(peers, *rpcTimeout, *grace, *gateway, *reporte)
	if err != nil {
		log.Fatalf("[BROKER] init: %v", err)
	}

	lis, err := net.Listen("tcp", ":"+*puerto)
	if err != nil {
		log.Fatalf("[BROKER] no pude escuchar en :%s: %v", *puerto, err)
	}

	srv := grpc.NewServer()
	pb.RegisterBrokerServiceServer(srv, b)

	go b.healthCheck(*health)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-b.stop:
			b.log.Printf("Shutdown completo, deteniendo servidor...")
		case <-sig:
			if err := b.GenerarReporte(); err != nil {
				b.log.Printf("error generando Reporte.txt en cierre: %v", err)
			}
		}
		srv.GracefulStop()
		os.Exit(0)
	}()

	b.log.Printf("escuchando gRPC en :%s | Datanodes=%d | health=%s | grace=%s", *puerto, len(peers), *health, *grace)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("[BROKER] gRPC terminó: %v", err)
	}
}
