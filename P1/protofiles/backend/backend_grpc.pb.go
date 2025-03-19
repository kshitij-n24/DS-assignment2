// protofiles/backend.proto

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v3.12.4
// source: backend.proto

package backend

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ComputationalService_ComputeTask_FullMethodName = "/backend.ComputationalService/ComputeTask"
)

// ComputationalServiceClient is the client API for ComputationalService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// The computational service performing arithmetic tasks.
type ComputationalServiceClient interface {
	// ComputeTask performs an operation.
	ComputeTask(ctx context.Context, in *ComputeTaskRequest, opts ...grpc.CallOption) (*ComputeTaskResponse, error)
}

type computationalServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewComputationalServiceClient(cc grpc.ClientConnInterface) ComputationalServiceClient {
	return &computationalServiceClient{cc}
}

func (c *computationalServiceClient) ComputeTask(ctx context.Context, in *ComputeTaskRequest, opts ...grpc.CallOption) (*ComputeTaskResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ComputeTaskResponse)
	err := c.cc.Invoke(ctx, ComputationalService_ComputeTask_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ComputationalServiceServer is the server API for ComputationalService service.
// All implementations must embed UnimplementedComputationalServiceServer
// for forward compatibility.
//
// The computational service performing arithmetic tasks.
type ComputationalServiceServer interface {
	// ComputeTask performs an operation.
	ComputeTask(context.Context, *ComputeTaskRequest) (*ComputeTaskResponse, error)
	mustEmbedUnimplementedComputationalServiceServer()
}

// UnimplementedComputationalServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedComputationalServiceServer struct{}

func (UnimplementedComputationalServiceServer) ComputeTask(context.Context, *ComputeTaskRequest) (*ComputeTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ComputeTask not implemented")
}
func (UnimplementedComputationalServiceServer) mustEmbedUnimplementedComputationalServiceServer() {}
func (UnimplementedComputationalServiceServer) testEmbeddedByValue()                              {}

// UnsafeComputationalServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ComputationalServiceServer will
// result in compilation errors.
type UnsafeComputationalServiceServer interface {
	mustEmbedUnimplementedComputationalServiceServer()
}

func RegisterComputationalServiceServer(s grpc.ServiceRegistrar, srv ComputationalServiceServer) {
	// If the following call pancis, it indicates UnimplementedComputationalServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ComputationalService_ServiceDesc, srv)
}

func _ComputationalService_ComputeTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ComputeTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputationalServiceServer).ComputeTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ComputationalService_ComputeTask_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputationalServiceServer).ComputeTask(ctx, req.(*ComputeTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ComputationalService_ServiceDesc is the grpc.ServiceDesc for ComputationalService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ComputationalService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "backend.ComputationalService",
	HandlerType: (*ComputationalServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ComputeTask",
			Handler:    _ComputationalService_ComputeTask_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "backend.proto",
}
