package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"
)

var restaurantes = []string{"BurgerNode", "DockerPizza", "SushiStream", "GopherTacos"}

func main() {
	id := flag.String("id", util.EnvOr("CLI_ID", "1"), "ID del cliente")
	gateway := flag.String("gateway", util.EnvOr("CLI_GATEWAY", "localhost:50040"), "dirección del Gateway")
	pedidos := flag.Int("pedidos", util.EnvIntOr("CLI_PEDIDOS", 2), "cantidad de pedidos a generar")
	intervalo := flag.Duration("intervalo", util.EnvDurationOr("CLI_INTERVALO", 2*time.Second), "espera entre pedidos")
	delayInicial := flag.Duration("delay-inicial", util.EnvDurationOr("CLI_DELAY", 4*time.Second), "espera inicial de arranque")
	rpcTimeout := flag.Duration("rpc-timeout", util.EnvDurationOr("CLI_RPC_TIMEOUT", 5*time.Second), "timeout de RPC")
	flag.Parse()

	lg := log.New(os.Stdout, fmt.Sprintf("[CLIENTE-%s] ", *id), log.LstdFlags|log.Lmicroseconds)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(*id))))

	conn, err := util.Dial(*gateway)
	if err != nil {
		log.Fatalf("[CLIENTE-%s] no pude conectar al Gateway: %v", *id, err)
	}
	defer conn.Close()
	client := pb.NewGatewayServiceClient(conn)

	lg.Printf("arrancando; espero %s a que el sistema esté listo...", *delayInicial)
	time.Sleep(*delayInicial)

	okRYW := 0
	for i := 1; i <= *pedidos; i++ {
		orderID := fmt.Sprintf("Ped-C%s-%03d", *id, i)
		rest := restaurantes[rnd.Intn(len(restaurantes))]
		if hacerPedido(client, lg, *id, orderID, rest, *rpcTimeout) {
			okRYW++
		}
		if i < *pedidos {
			time.Sleep(*intervalo)
		}}
	lg.Printf("FIN: %d/%d pedidos con RYW validado", okRYW, *pedidos)
}


func hacerPedido(client pb.GatewayServiceClient, lg *log.Logger, clientID, orderID, restaurante string, timeout time.Duration) bool {
	reqID := util.GenID("req-" + clientID)

	wctx, cancel := util.CtxTimeout(timeout)
	cResp, err := client.CrearPedido(wctx, &pb.CrearPedidoRequest{
		RequestId:  reqID,
		ClientId:   clientID,
		OrderId:    orderID,
		Restaurant: restaurante,
		Items:      []string{"item-1", "item-2"},
	})
	cancel()
	if err != nil {
		lg.Printf("CrearPedido %s -> ERROR de transporte: %v", orderID, err)
		return false
	}
	if !cResp.GetSuccess() {
		lg.Printf("CrearPedido %s -> RECHAZADO: %s (no valido RYW)", orderID, cResp.GetMessage())
		return false
	}
	lg.Printf("CrearPedido %s OK en %s (restaurante %s)", orderID, cResp.GetDatanodeId(), restaurante)

	rctx, cancel2 := util.CtxTimeout(timeout)
	qResp, err := client.ConsultarEstado(rctx, &pb.ConsultarEstadoRequest{ClientId: clientID, OrderId: orderID})
	cancel2()
	if err != nil {
		lg.Printf("ConsultarEstado %s -> ERROR de transporte: %v", orderID, err)
		return false
	}

	if qResp.GetFound() && qResp.GetOrder().GetOrderId() == orderID && qResp.GetFromAffinity() {
		lg.Printf("RYW OK: pedido %s confirmado en %s (estado %q, afinidad de sesion)",
			orderID, qResp.GetDatanodeId(), qResp.GetOrder().GetStatus())
		return true
	}
	lg.Printf("RYW FALLIDO: pedido %s no se leyó desde su Datanode afín (found=%v afinidad=%v datanode=%s)",
		orderID, qResp.GetFound(), qResp.GetFromAffinity(), qResp.GetDatanodeId())
	return false
}
