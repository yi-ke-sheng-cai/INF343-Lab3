package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)


const _ = grpc.SupportPackageIsVersion9

const (
	GatewayService_CrearPedido_FullMethodName         = "/distrieats.GatewayService/CrearPedido"
	GatewayService_ConsultarEstado_FullMethodName     = "/distrieats.GatewayService/ConsultarEstado"
	GatewayService_ObtenerAuditoriaRYW_FullMethodName = "/distrieats.GatewayService/ObtenerAuditoriaRYW"
)


type GatewayServiceClient interface {
	CrearPedido(ctx context.Context, in *CrearPedidoRequest, opts ...grpc.CallOption) (*CrearPedidoResponse, error)
	ConsultarEstado(ctx context.Context, in *ConsultarEstadoRequest, opts ...grpc.CallOption) (*ConsultarEstadoResponse, error)
	ObtenerAuditoriaRYW(ctx context.Context, in *AuditoriaRYWRequest, opts ...grpc.CallOption) (*AuditoriaRYWResponse, error)
}

type gatewayServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewGatewayServiceClient(cc grpc.ClientConnInterface) GatewayServiceClient {
	return &gatewayServiceClient{cc}
}

func (c *gatewayServiceClient) CrearPedido(ctx context.Context, in *CrearPedidoRequest, opts ...grpc.CallOption) (*CrearPedidoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CrearPedidoResponse)
	err := c.cc.Invoke(ctx, GatewayService_CrearPedido_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) ConsultarEstado(ctx context.Context, in *ConsultarEstadoRequest, opts ...grpc.CallOption) (*ConsultarEstadoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ConsultarEstadoResponse)
	err := c.cc.Invoke(ctx, GatewayService_ConsultarEstado_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) ObtenerAuditoriaRYW(ctx context.Context, in *AuditoriaRYWRequest, opts ...grpc.CallOption) (*AuditoriaRYWResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AuditoriaRYWResponse)
	err := c.cc.Invoke(ctx, GatewayService_ObtenerAuditoriaRYW_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}


type GatewayServiceServer interface {
	CrearPedido(context.Context, *CrearPedidoRequest) (*CrearPedidoResponse, error)
	ConsultarEstado(context.Context, *ConsultarEstadoRequest) (*ConsultarEstadoResponse, error)
	ObtenerAuditoriaRYW(context.Context, *AuditoriaRYWRequest) (*AuditoriaRYWResponse, error)
	mustEmbedUnimplementedGatewayServiceServer()
}


type UnimplementedGatewayServiceServer struct{}

func (UnimplementedGatewayServiceServer) CrearPedido(context.Context, *CrearPedidoRequest) (*CrearPedidoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CrearPedido not implemented")}
func (UnimplementedGatewayServiceServer) ConsultarEstado(context.Context, *ConsultarEstadoRequest) (*ConsultarEstadoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ConsultarEstado not implemented")}
func (UnimplementedGatewayServiceServer) ObtenerAuditoriaRYW(context.Context, *AuditoriaRYWRequest) (*AuditoriaRYWResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ObtenerAuditoriaRYW not implemented")}
func (UnimplementedGatewayServiceServer) mustEmbedUnimplementedGatewayServiceServer() {}
func (UnimplementedGatewayServiceServer) testEmbeddedByValue(){}


type UnsafeGatewayServiceServer interface {
	mustEmbedUnimplementedGatewayServiceServer()
}

func RegisterGatewayServiceServer(s grpc.ServiceRegistrar, srv GatewayServiceServer) {
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&GatewayService_ServiceDesc, srv)
}

func _GatewayService_CrearPedido_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CrearPedidoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).CrearPedido(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_CrearPedido_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).CrearPedido(ctx, req.(*CrearPedidoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_ConsultarEstado_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConsultarEstadoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).ConsultarEstado(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_ConsultarEstado_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).ConsultarEstado(ctx, req.(*ConsultarEstadoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_ObtenerAuditoriaRYW_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuditoriaRYWRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).ObtenerAuditoriaRYW(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_ObtenerAuditoriaRYW_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).ObtenerAuditoriaRYW(ctx, req.(*AuditoriaRYWRequest))
	}
	return interceptor(ctx, in, info, handler)
}


var GatewayService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "distrieats.GatewayService",
	HandlerType: (*GatewayServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CrearPedido",
			Handler:    _GatewayService_CrearPedido_Handler,
		},
		{
			MethodName: "ConsultarEstado",
			Handler:    _GatewayService_ConsultarEstado_Handler,
		},
		{
			MethodName: "ObtenerAuditoriaRYW",
			Handler:    _GatewayService_ObtenerAuditoriaRYW_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/distrieats.proto",
}

const (
	BrokerService_EnrutarEscritura_FullMethodName      = "/distrieats.BrokerService/EnrutarEscritura"
	BrokerService_EnrutarLectura_FullMethodName        = "/distrieats.BrokerService/EnrutarLectura"
	BrokerService_EmitirEventoLogistico_FullMethodName = "/distrieats.BrokerService/EmitirEventoLogistico"
	BrokerService_SenalarFinEventos_FullMethodName     = "/distrieats.BrokerService/SenalarFinEventos"
)

type BrokerServiceClient interface {
	EnrutarEscritura(ctx context.Context, in *UpdateOrderRequest, opts ...grpc.CallOption) (*UpdateOrderResponse, error)
	EnrutarLectura(ctx context.Context, in *GetOrderRequest, opts ...grpc.CallOption) (*GetOrderResponse, error)
	EmitirEventoLogistico(ctx context.Context, in *UpdateOrderRequest, opts ...grpc.CallOption) (*UpdateOrderResponse, error)
	SenalarFinEventos(ctx context.Context, in *FinEventosRequest, opts ...grpc.CallOption) (*FinEventosResponse, error)
}

type brokerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBrokerServiceClient(cc grpc.ClientConnInterface) BrokerServiceClient {
	return &brokerServiceClient{cc}
}

func (c *brokerServiceClient) EnrutarEscritura(ctx context.Context, in *UpdateOrderRequest, opts ...grpc.CallOption) (*UpdateOrderResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpdateOrderResponse)
	err := c.cc.Invoke(ctx, BrokerService_EnrutarEscritura_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerServiceClient) EnrutarLectura(ctx context.Context, in *GetOrderRequest, opts ...grpc.CallOption) (*GetOrderResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetOrderResponse)
	err := c.cc.Invoke(ctx, BrokerService_EnrutarLectura_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerServiceClient) EmitirEventoLogistico(ctx context.Context, in *UpdateOrderRequest, opts ...grpc.CallOption) (*UpdateOrderResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpdateOrderResponse)
	err := c.cc.Invoke(ctx, BrokerService_EmitirEventoLogistico_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerServiceClient) SenalarFinEventos(ctx context.Context, in *FinEventosRequest, opts ...grpc.CallOption) (*FinEventosResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(FinEventosResponse)
	err := c.cc.Invoke(ctx, BrokerService_SenalarFinEventos_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}


type BrokerServiceServer interface {
	EnrutarEscritura(context.Context, *UpdateOrderRequest) (*UpdateOrderResponse, error)
	EnrutarLectura(context.Context, *GetOrderRequest) (*GetOrderResponse, error)
	EmitirEventoLogistico(context.Context, *UpdateOrderRequest) (*UpdateOrderResponse, error)
	SenalarFinEventos(context.Context, *FinEventosRequest) (*FinEventosResponse, error)
	mustEmbedUnimplementedBrokerServiceServer()
}


type UnimplementedBrokerServiceServer struct{}

func (UnimplementedBrokerServiceServer) EnrutarEscritura(context.Context, *UpdateOrderRequest) (*UpdateOrderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method EnrutarEscritura not implemented")}
func (UnimplementedBrokerServiceServer) EnrutarLectura(context.Context, *GetOrderRequest) (*GetOrderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method EnrutarLectura not implemented")}
func (UnimplementedBrokerServiceServer) EmitirEventoLogistico(context.Context, *UpdateOrderRequest) (*UpdateOrderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method EmitirEventoLogistico not implemented")}
func (UnimplementedBrokerServiceServer) SenalarFinEventos(context.Context, *FinEventosRequest) (*FinEventosResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SenalarFinEventos not implemented")}
func (UnimplementedBrokerServiceServer) mustEmbedUnimplementedBrokerServiceServer() {}
func (UnimplementedBrokerServiceServer) testEmbeddedByValue()                       {}


type UnsafeBrokerServiceServer interface {
	mustEmbedUnimplementedBrokerServiceServer()
}

func RegisterBrokerServiceServer(s grpc.ServiceRegistrar, srv BrokerServiceServer) {
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&BrokerService_ServiceDesc, srv)
}

func _BrokerService_EnrutarEscritura_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateOrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServiceServer).EnrutarEscritura(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BrokerService_EnrutarEscritura_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServiceServer).EnrutarEscritura(ctx, req.(*UpdateOrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BrokerService_EnrutarLectura_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServiceServer).EnrutarLectura(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BrokerService_EnrutarLectura_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServiceServer).EnrutarLectura(ctx, req.(*GetOrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BrokerService_EmitirEventoLogistico_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateOrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServiceServer).EmitirEventoLogistico(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BrokerService_EmitirEventoLogistico_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServiceServer).EmitirEventoLogistico(ctx, req.(*UpdateOrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BrokerService_SenalarFinEventos_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FinEventosRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServiceServer).SenalarFinEventos(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BrokerService_SenalarFinEventos_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServiceServer).SenalarFinEventos(ctx, req.(*FinEventosRequest))
	}
	return interceptor(ctx, in, info, handler)
}


var BrokerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "distrieats.BrokerService",
	HandlerType: (*BrokerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "EnrutarEscritura",
			Handler:    _BrokerService_EnrutarEscritura_Handler,},
		{MethodName: "EnrutarLectura",
			Handler:    _BrokerService_EnrutarLectura_Handler,},
		{MethodName: "EmitirEventoLogistico",
			Handler:    _BrokerService_EmitirEventoLogistico_Handler,},
		{MethodName: "SenalarFinEventos",
			Handler:    _BrokerService_SenalarFinEventos_Handler,},},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/distrieats.proto",
}

const (
	DatanodeService_UpdateOrder_FullMethodName = "/distrieats.DatanodeService/UpdateOrder"
	DatanodeService_GetOrder_FullMethodName    = "/distrieats.DatanodeService/GetOrder"
	DatanodeService_GossipSync_FullMethodName  = "/distrieats.DatanodeService/GossipSync"
	DatanodeService_Ping_FullMethodName        = "/distrieats.DatanodeService/Ping"
	DatanodeService_Snapshot_FullMethodName    = "/distrieats.DatanodeService/Snapshot"
)


type DatanodeServiceClient interface {
	UpdateOrder(ctx context.Context, in *UpdateOrderRequest, opts ...grpc.CallOption) (*UpdateOrderResponse, error)
	GetOrder(ctx context.Context, in *GetOrderRequest, opts ...grpc.CallOption) (*GetOrderResponse, error)
	GossipSync(ctx context.Context, in *GossipSyncRequest, opts ...grpc.CallOption) (*GossipSyncResponse, error)
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
	Snapshot(ctx context.Context, in *SnapshotRequest, opts ...grpc.CallOption) (*SnapshotResponse, error)
}

type datanodeServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDatanodeServiceClient(cc grpc.ClientConnInterface) DatanodeServiceClient {
	return &datanodeServiceClient{cc}
}

func (c *datanodeServiceClient) UpdateOrder(ctx context.Context, in *UpdateOrderRequest, opts ...grpc.CallOption) (*UpdateOrderResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpdateOrderResponse)
	err := c.cc.Invoke(ctx, DatanodeService_UpdateOrder_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datanodeServiceClient) GetOrder(ctx context.Context, in *GetOrderRequest, opts ...grpc.CallOption) (*GetOrderResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetOrderResponse)
	err := c.cc.Invoke(ctx, DatanodeService_GetOrder_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datanodeServiceClient) GossipSync(ctx context.Context, in *GossipSyncRequest, opts ...grpc.CallOption) (*GossipSyncResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GossipSyncResponse)
	err := c.cc.Invoke(ctx, DatanodeService_GossipSync_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datanodeServiceClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, DatanodeService_Ping_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datanodeServiceClient) Snapshot(ctx context.Context, in *SnapshotRequest, opts ...grpc.CallOption) (*SnapshotResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SnapshotResponse)
	err := c.cc.Invoke(ctx, DatanodeService_Snapshot_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}


type DatanodeServiceServer interface {
	UpdateOrder(context.Context, *UpdateOrderRequest) (*UpdateOrderResponse, error)
	GetOrder(context.Context, *GetOrderRequest) (*GetOrderResponse, error)
	GossipSync(context.Context, *GossipSyncRequest) (*GossipSyncResponse, error)
	Ping(context.Context, *PingRequest) (*PingResponse, error)
	Snapshot(context.Context, *SnapshotRequest) (*SnapshotResponse, error)
	mustEmbedUnimplementedDatanodeServiceServer()
}


type UnimplementedDatanodeServiceServer struct{}

func (UnimplementedDatanodeServiceServer) UpdateOrder(context.Context, *UpdateOrderRequest) (*UpdateOrderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method UpdateOrder not implemented")}
func (UnimplementedDatanodeServiceServer) GetOrder(context.Context, *GetOrderRequest) (*GetOrderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetOrder not implemented")}
func (UnimplementedDatanodeServiceServer) GossipSync(context.Context, *GossipSyncRequest) (*GossipSyncResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GossipSync not implemented")}
func (UnimplementedDatanodeServiceServer) Ping(context.Context, *PingRequest) (*PingResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Ping not implemented")}
func (UnimplementedDatanodeServiceServer) Snapshot(context.Context, *SnapshotRequest) (*SnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Snapshot not implemented")}
func (UnimplementedDatanodeServiceServer) mustEmbedUnimplementedDatanodeServiceServer() {}
func (UnimplementedDatanodeServiceServer) testEmbeddedByValue()                         {}


type UnsafeDatanodeServiceServer interface {
	mustEmbedUnimplementedDatanodeServiceServer()
}

func RegisterDatanodeServiceServer(s grpc.ServiceRegistrar, srv DatanodeServiceServer) {
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&DatanodeService_ServiceDesc, srv)
}

func _DatanodeService_UpdateOrder_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateOrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatanodeServiceServer).UpdateOrder(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DatanodeService_UpdateOrder_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatanodeServiceServer).UpdateOrder(ctx, req.(*UpdateOrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatanodeService_GetOrder_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOrderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatanodeServiceServer).GetOrder(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DatanodeService_GetOrder_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatanodeServiceServer).GetOrder(ctx, req.(*GetOrderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatanodeService_GossipSync_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GossipSyncRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatanodeServiceServer).GossipSync(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DatanodeService_GossipSync_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatanodeServiceServer).GossipSync(ctx, req.(*GossipSyncRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatanodeService_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatanodeServiceServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DatanodeService_Ping_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatanodeServiceServer).Ping(ctx, req.(*PingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatanodeService_Snapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatanodeServiceServer).Snapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DatanodeService_Snapshot_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatanodeServiceServer).Snapshot(ctx, req.(*SnapshotRequest))
	}
	return interceptor(ctx, in, info, handler)
}


var DatanodeService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "distrieats.DatanodeService",
	HandlerType: (*DatanodeServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "UpdateOrder",
			Handler:    _DatanodeService_UpdateOrder_Handler,},
		{MethodName: "GetOrder",
			Handler:    _DatanodeService_GetOrder_Handler,},
		{MethodName: "GossipSync",
			Handler:    _DatanodeService_GossipSync_Handler,},
		{MethodName: "Ping",
			Handler:    _DatanodeService_Ping_Handler,},
		{MethodName: "Snapshot",
			Handler:    _DatanodeService_Snapshot_Handler,},},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/distrieats.proto",
}
