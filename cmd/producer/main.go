// Command producer es el emisor de eventos logísticos (Restaurante/Repartidor):
// lee pedidos.csv y va enviando cada transición de estado al Broker, que la
// enruta a los Datanodes. Los eventos que comparten tiempo_relativo se emiten
// en ráfaga (concurrentes) para ejercitar la resolución de conflictos; entre
// grupos de tiempo distinto espera un intervalo aleatorio configurable.
//
// Configuración (flag > env var > default):
//
//	-broker      dirección del Broker                     env PROD_BROKER
//	-csv         ruta del pedidos.csv                     env PROD_CSV
//	-min / -max  espera aleatoria entre grupos de eventos env PROD_MIN/PROD_MAX
//	-delay-inicial  espera antes de arrancar              env PROD_DELAY
//	-rpc-timeout timeout de RPC                            env PROD_RPC_TIMEOUT
package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"
)

func main() {
	broker := flag.String("broker", util.EnvOr("PROD_BROKER", "localhost:50051"), "dirección del Broker")
	csvPath := flag.String("csv", util.EnvOr("PROD_CSV", "data/pedidos_pequeño.csv"), "ruta del pedidos.csv")
	minWait := flag.Duration("min", util.EnvDurationOr("PROD_MIN", 1*time.Second), "espera mínima entre grupos")
	maxWait := flag.Duration("max", util.EnvDurationOr("PROD_MAX", 3*time.Second), "espera máxima entre grupos")
	delayInicial := flag.Duration("delay-inicial", util.EnvDurationOr("PROD_DELAY", 3*time.Second), "espera inicial de arranque")
	rpcTimeout := flag.Duration("rpc-timeout", util.EnvDurationOr("PROD_RPC_TIMEOUT", 5*time.Second), "timeout de RPC")
	flag.Parse()

	lg := log.New(os.Stdout, "[PRODUCTOR] ", log.LstdFlags|log.Lmicroseconds)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	eventos, err := LeerEventos(*csvPath, lg)
	if err != nil {
		log.Fatalf("[PRODUCTOR] no pude leer CSV %s: %v", *csvPath, err)
	}
	lg.Printf("CSV %s cargado: %d eventos válidos", *csvPath, len(eventos))

	conn, err := util.Dial(*broker)
	if err != nil {
		log.Fatalf("[PRODUCTOR] no pude conectar al Broker: %v", err)
	}
	defer conn.Close()
	client := pb.NewBrokerServiceClient(conn)

	lg.Printf("arrancando; espero %s a que el sistema esté listo...", *delayInicial)
	time.Sleep(*delayInicial)

	emitidos := 0
	prevT := -1
	for _, ev := range eventos {
		// Nuevo grupo temporal: esperar un intervalo aleatorio antes de emitirlo.
		if ev.T != prevT && prevT != -1 {
			wait := *minWait
			if *maxWait > *minWait {
				wait += time.Duration(rnd.Int63n(int64(*maxWait - *minWait)))
			}
			time.Sleep(wait)
		}
		prevT = ev.T

		order := &pb.Order{
			OrderId:    ev.OrderID,
			Restaurant: ev.Restaurant,
			Status:     ev.Status,
			Timestamp:  time.Now().UnixNano(),
		}
		ctx, cancel := util.CtxTimeout(*rpcTimeout)
		resp, err := client.EmitirEventoLogistico(ctx, &pb.UpdateOrderRequest{
			Order:     order,
			RequestId: util.GenID("evt"),
		})
		cancel()
		if err != nil {
			lg.Printf("evento %s->%q (t=%d) ERROR: %v", ev.OrderID, ev.Status, ev.T, err)
			continue
		}
		emitidos++
		lg.Printf("evento %s->%q (t=%d, %s) enviado -> %s aplicado=%v",
			ev.OrderID, ev.Status, ev.T, ev.Actor, resp.GetDatanodeId(), resp.GetApplied())
	}

	lg.Printf("todos los eventos emitidos (%d/%d). Señalando FIN al Broker.", emitidos, len(eventos))
	ctx, cancel := util.CtxTimeout(*rpcTimeout)
	if _, err := client.SenalarFinEventos(ctx, &pb.FinEventosRequest{TotalEventos: int32(emitidos)}); err != nil {
		lg.Printf("no pude señalar fin de eventos: %v", err)
	}
	cancel()
	lg.Printf("productor finalizado.")
}
