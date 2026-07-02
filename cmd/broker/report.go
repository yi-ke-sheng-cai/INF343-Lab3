package main

import (
	"fmt"
	"os"

	pb "distrieats/proto/pb"

	"distrieats/internal/util"
	"distrieats/internal/vclock"
)


func (b *Broker) GenerarReporte() error {
	if b.reported.Load() {
		b.log.Printf("Reporte: ya existe un reporte final; omito regeneración")
		return nil
	}

	orders := b.snapshotDesdeDatanode()
	ryw := b.auditoriaDesdeGateway()

	f, err := os.Create(b.reportPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintln(f, "=== REPORTE FINAL : DISTRIEATS ===")
	fmt.Fprintln(f)
	fmt.Fprintln(f, "[ESTADO GLOBAL DE PEDIDOS - Convergencia Alcanzada]")
	for _, o := range orders {
		fmt.Fprintf(f, "Pedido ID: %s | Estado Final: %s | Reloj Vectorial: %s\n",
			o.GetOrderId(), o.GetStatus(), vclock.String(o.GetClock()))
	}
	fmt.Fprintln(f)
	fmt.Fprintln(f, "[AUDITORIA READ YOUR WRITES]")
	if len(ryw) == 0 {
		fmt.Fprintln(f, "- (sin validaciones registradas)")
	}
	for _, e := range ryw {
		fmt.Fprintf(f, "- Cliente %s (%s): Validacion Exitosa en %s (Afinidad de sesion confirmada).\n",
			e.GetClientId(), e.GetOrderId(), e.GetDatanodeId())
	}
	fmt.Fprintln(f, "=================================")

	if len(orders) > 0 || len(ryw) > 0 {
		b.reported.Store(true)
	}
	b.log.Printf("Reporte.txt generado en %s (%d pedidos, %d validaciones RYW)", b.reportPath, len(orders), len(ryw))

	b.log.Printf("Enviando señal de Shutdown a todos los Datanodes...")
	for _, ns := range b.nodes {
		ctx, cancel := util.CtxTimeout(b.dial)
		_, err := ns.client.Shutdown(ctx, &pb.PingRequest{})
		cancel()
		if err != nil {
			b.log.Printf("Shutdown %s: %v", ns.peer.ID, err)
		} else {
			b.log.Printf("Shutdown %s: OK", ns.peer.ID)
		}
	}

	if b.gatewayAddr != "" {
		b.log.Printf("Enviando señal de Shutdown al Gateway...")
		gwConn, err := util.Dial(b.gatewayAddr)
		if err != nil {
			b.log.Printf("Shutdown Gateway: no pude conectar: %v", err)
		} else {
			ctx, cancel := util.CtxTimeout(b.dial)
			_, err = pb.NewGatewayServiceClient(gwConn).Shutdown(ctx, &pb.PingRequest{})
			cancel()
			gwConn.Close()
			if err != nil {
				b.log.Printf("Shutdown Gateway: %v", err)
			} else {
				b.log.Printf("Shutdown Gateway: OK")
			}
		}
	}
	return nil
}
func (b *Broker) snapshotDesdeDatanode() []*pb.Order {
	for _, ns := range b.nodes {
		if !ns.alive.Load() {
			continue
		}
		ctx, cancel := util.CtxTimeout(b.dial)
		resp, err := ns.client.Snapshot(ctx, &pb.SnapshotRequest{})
		cancel()
		if err == nil {
			return resp.GetOrders()
		}
		b.log.Printf("Reporte: %s no entregó snapshot: %v", ns.peer.ID, err)
	}
	return nil
}


func (b *Broker) auditoriaDesdeGateway() []*pb.RYWEntry {
	if b.gatewayAddr == "" {
		return nil
	}
	conn, err := util.Dial(b.gatewayAddr)
	if err != nil {
		b.log.Printf("Reporte: no pude conectar al Gateway (%s): %v", b.gatewayAddr, err)
		return nil
	}
	defer conn.Close()
	ctx, cancel := util.CtxTimeout(b.dial)
	defer cancel()
	resp, err := pb.NewGatewayServiceClient(conn).ObtenerAuditoriaRYW(ctx, &pb.AuditoriaRYWRequest{})
	if err != nil {
		b.log.Printf("Reporte: Gateway no entregó auditoría RYW: %v", err)
		return nil
	}
	return resp.GetEntries()
}
