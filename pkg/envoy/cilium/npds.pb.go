// Code generated by protoc-gen-go. DO NOT EDIT.
// source: cilium/npds.proto

package cilium

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import envoy_api_v2_core3 "github.com/cilium/cilium/pkg/envoy/envoy/api/v2/core"
import envoy_api_v2 "github.com/cilium/cilium/pkg/envoy/envoy/api/v2"
import envoy_api_v2_route "github.com/cilium/cilium/pkg/envoy/envoy/api/v2/route"
import _ "google.golang.org/genproto/googleapis/api/annotations"
import _ "github.com/lyft/protoc-gen-validate/validate"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// A network policy that is enforced by a filter on the network flows to/from
// associated hosts.
type NetworkPolicy struct {
	// The unique identifier of the network policy.
	// Required.
	Policy uint64 `protobuf:"varint,1,opt,name=policy" json:"policy,omitempty"`
	// The part of the policy to be enforced at ingress by the filter, as a set
	// of per-port network policies, one per destination L4 port.
	// Every PortNetworkPolicy element in this set has a unique port / protocol
	// combination.
	// Optional. If empty, all flows in this direction are denied.
	IngressPerPortPolicies []*PortNetworkPolicy `protobuf:"bytes,2,rep,name=ingress_per_port_policies,json=ingressPerPortPolicies" json:"ingress_per_port_policies,omitempty"`
	// The part of the policy to be enforced at egress by the filter, as a set
	// of per-port network policies, one per destination L4 port.
	// Every PortNetworkPolicy element in this set has a unique port / protocol
	// combination.
	// Optional. If empty, all flows in this direction are denied.
	EgressPerPortPolicies []*PortNetworkPolicy `protobuf:"bytes,3,rep,name=egress_per_port_policies,json=egressPerPortPolicies" json:"egress_per_port_policies,omitempty"`
}

func (m *NetworkPolicy) Reset()                    { *m = NetworkPolicy{} }
func (m *NetworkPolicy) String() string            { return proto.CompactTextString(m) }
func (*NetworkPolicy) ProtoMessage()               {}
func (*NetworkPolicy) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{0} }

func (m *NetworkPolicy) GetPolicy() uint64 {
	if m != nil {
		return m.Policy
	}
	return 0
}

func (m *NetworkPolicy) GetIngressPerPortPolicies() []*PortNetworkPolicy {
	if m != nil {
		return m.IngressPerPortPolicies
	}
	return nil
}

func (m *NetworkPolicy) GetEgressPerPortPolicies() []*PortNetworkPolicy {
	if m != nil {
		return m.EgressPerPortPolicies
	}
	return nil
}

// A network policy to whitelist flows to a specific destination L4 port,
// as a conjunction of predicates on L3/L4/L7 flows.
// If all the predicates of a policy match a flow, the flow is whitelisted.
type PortNetworkPolicy struct {
	// The flows' destination L4 port number, as an unsigned 16-bit integer.
	// If 0, all destination L4 port numbers are matched by this predicate.
	Port uint32 `protobuf:"varint,1,opt,name=port" json:"port,omitempty"`
	// The flows' L4 transport protocol.
	// Required.
	Protocol envoy_api_v2_core3.SocketAddress_Protocol `protobuf:"varint,2,opt,name=protocol,enum=envoy.api.v2.core.SocketAddress_Protocol" json:"protocol,omitempty"`
	// The network policy rules to be enforced on the flows to the port.
	// Optional. A flow is matched by this predicate if either the set of
	// rules is empty or any of the rules matches it.
	Rules []*PortNetworkPolicyRule `protobuf:"bytes,3,rep,name=rules" json:"rules,omitempty"`
}

func (m *PortNetworkPolicy) Reset()                    { *m = PortNetworkPolicy{} }
func (m *PortNetworkPolicy) String() string            { return proto.CompactTextString(m) }
func (*PortNetworkPolicy) ProtoMessage()               {}
func (*PortNetworkPolicy) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{1} }

func (m *PortNetworkPolicy) GetPort() uint32 {
	if m != nil {
		return m.Port
	}
	return 0
}

func (m *PortNetworkPolicy) GetProtocol() envoy_api_v2_core3.SocketAddress_Protocol {
	if m != nil {
		return m.Protocol
	}
	return envoy_api_v2_core3.SocketAddress_TCP
}

func (m *PortNetworkPolicy) GetRules() []*PortNetworkPolicyRule {
	if m != nil {
		return m.Rules
	}
	return nil
}

// A network policy rule, as a conjunction of predicates on L3/L7 flows.
// If all the predicates of a rule match a flow, the flow is matched by the
// rule.
type PortNetworkPolicyRule struct {
	// The set of identifiers of policies of remote hosts.
	// A flow is matched by this predicate if the identifier of the policy
	// applied on the flow's remote host is contained in this set.
	// Optional. If not specified, any remote host is matched by this predicate.
	RemotePolicies []uint64 `protobuf:"varint,1,rep,packed,name=remote_policies,json=remotePolicies" json:"remote_policies,omitempty"`
	// Optional. If not specified, any L7 request is matched by this predicate.
	//
	// Types that are valid to be assigned to L7Rules:
	//	*PortNetworkPolicyRule_HttpRules
	L7Rules isPortNetworkPolicyRule_L7Rules `protobuf_oneof:"l7_rules"`
}

func (m *PortNetworkPolicyRule) Reset()                    { *m = PortNetworkPolicyRule{} }
func (m *PortNetworkPolicyRule) String() string            { return proto.CompactTextString(m) }
func (*PortNetworkPolicyRule) ProtoMessage()               {}
func (*PortNetworkPolicyRule) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{2} }

type isPortNetworkPolicyRule_L7Rules interface {
	isPortNetworkPolicyRule_L7Rules()
}

type PortNetworkPolicyRule_HttpRules struct {
	HttpRules *HttpNetworkPolicyRules `protobuf:"bytes,100,opt,name=http_rules,json=httpRules,oneof"`
}

func (*PortNetworkPolicyRule_HttpRules) isPortNetworkPolicyRule_L7Rules() {}

func (m *PortNetworkPolicyRule) GetL7Rules() isPortNetworkPolicyRule_L7Rules {
	if m != nil {
		return m.L7Rules
	}
	return nil
}

func (m *PortNetworkPolicyRule) GetRemotePolicies() []uint64 {
	if m != nil {
		return m.RemotePolicies
	}
	return nil
}

func (m *PortNetworkPolicyRule) GetHttpRules() *HttpNetworkPolicyRules {
	if x, ok := m.GetL7Rules().(*PortNetworkPolicyRule_HttpRules); ok {
		return x.HttpRules
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*PortNetworkPolicyRule) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _PortNetworkPolicyRule_OneofMarshaler, _PortNetworkPolicyRule_OneofUnmarshaler, _PortNetworkPolicyRule_OneofSizer, []interface{}{
		(*PortNetworkPolicyRule_HttpRules)(nil),
	}
}

func _PortNetworkPolicyRule_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*PortNetworkPolicyRule)
	// l7_rules
	switch x := m.L7Rules.(type) {
	case *PortNetworkPolicyRule_HttpRules:
		b.EncodeVarint(100<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.HttpRules); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("PortNetworkPolicyRule.L7Rules has unexpected type %T", x)
	}
	return nil
}

func _PortNetworkPolicyRule_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*PortNetworkPolicyRule)
	switch tag {
	case 100: // l7_rules.http_rules
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(HttpNetworkPolicyRules)
		err := b.DecodeMessage(msg)
		m.L7Rules = &PortNetworkPolicyRule_HttpRules{msg}
		return true, err
	default:
		return false, nil
	}
}

func _PortNetworkPolicyRule_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*PortNetworkPolicyRule)
	// l7_rules
	switch x := m.L7Rules.(type) {
	case *PortNetworkPolicyRule_HttpRules:
		s := proto.Size(x.HttpRules)
		n += proto.SizeVarint(100<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

// A set of network policy rules that match HTTP requests.
type HttpNetworkPolicyRules struct {
	// The set of HTTP network policy rules.
	// An HTTP request is matched if any of its rules matches the request.
	// Required and may not be empty.
	HttpRules []*HttpNetworkPolicyRule `protobuf:"bytes,1,rep,name=http_rules,json=httpRules" json:"http_rules,omitempty"`
}

func (m *HttpNetworkPolicyRules) Reset()                    { *m = HttpNetworkPolicyRules{} }
func (m *HttpNetworkPolicyRules) String() string            { return proto.CompactTextString(m) }
func (*HttpNetworkPolicyRules) ProtoMessage()               {}
func (*HttpNetworkPolicyRules) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{3} }

func (m *HttpNetworkPolicyRules) GetHttpRules() []*HttpNetworkPolicyRule {
	if m != nil {
		return m.HttpRules
	}
	return nil
}

// An HTTP network policy rule, as a conjunction of predicates on HTTP requests.
// If all the predicates of a rule match an HTTP request, the request is allowed. Otherwise, it is
// denied.
type HttpNetworkPolicyRule struct {
	// A set of matchers on the HTTP request's headers' names and values.
	// If all the matchers in this set match an HTTP request, the request is allowed by this rule.
	// Otherwise, it is denied.
	//
	// Some special header names are:
	//
	// * *:uri*: The HTTP request's URI.
	// * *:method*: The HTTP request's method.
	// * *:authority*: Also maps to the HTTP 1.1 *Host* header.
	//
	// Optional. If empty, matches any HTTP request.
	Headers []*envoy_api_v2_route.HeaderMatcher `protobuf:"bytes,1,rep,name=headers" json:"headers,omitempty"`
}

func (m *HttpNetworkPolicyRule) Reset()                    { *m = HttpNetworkPolicyRule{} }
func (m *HttpNetworkPolicyRule) String() string            { return proto.CompactTextString(m) }
func (*HttpNetworkPolicyRule) ProtoMessage()               {}
func (*HttpNetworkPolicyRule) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{4} }

func (m *HttpNetworkPolicyRule) GetHeaders() []*envoy_api_v2_route.HeaderMatcher {
	if m != nil {
		return m.Headers
	}
	return nil
}

func init() {
	proto.RegisterType((*NetworkPolicy)(nil), "cilium.NetworkPolicy")
	proto.RegisterType((*PortNetworkPolicy)(nil), "cilium.PortNetworkPolicy")
	proto.RegisterType((*PortNetworkPolicyRule)(nil), "cilium.PortNetworkPolicyRule")
	proto.RegisterType((*HttpNetworkPolicyRules)(nil), "cilium.HttpNetworkPolicyRules")
	proto.RegisterType((*HttpNetworkPolicyRule)(nil), "cilium.HttpNetworkPolicyRule")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for NetworkPolicyDiscoveryService service

type NetworkPolicyDiscoveryServiceClient interface {
	StreamNetworkPolicies(ctx context.Context, opts ...grpc.CallOption) (NetworkPolicyDiscoveryService_StreamNetworkPoliciesClient, error)
	FetchNetworkPolicies(ctx context.Context, in *envoy_api_v2.DiscoveryRequest, opts ...grpc.CallOption) (*envoy_api_v2.DiscoveryResponse, error)
}

type networkPolicyDiscoveryServiceClient struct {
	cc *grpc.ClientConn
}

func NewNetworkPolicyDiscoveryServiceClient(cc *grpc.ClientConn) NetworkPolicyDiscoveryServiceClient {
	return &networkPolicyDiscoveryServiceClient{cc}
}

func (c *networkPolicyDiscoveryServiceClient) StreamNetworkPolicies(ctx context.Context, opts ...grpc.CallOption) (NetworkPolicyDiscoveryService_StreamNetworkPoliciesClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_NetworkPolicyDiscoveryService_serviceDesc.Streams[0], c.cc, "/cilium.NetworkPolicyDiscoveryService/StreamNetworkPolicies", opts...)
	if err != nil {
		return nil, err
	}
	x := &networkPolicyDiscoveryServiceStreamNetworkPoliciesClient{stream}
	return x, nil
}

type NetworkPolicyDiscoveryService_StreamNetworkPoliciesClient interface {
	Send(*envoy_api_v2.DiscoveryRequest) error
	Recv() (*envoy_api_v2.DiscoveryResponse, error)
	grpc.ClientStream
}

type networkPolicyDiscoveryServiceStreamNetworkPoliciesClient struct {
	grpc.ClientStream
}

func (x *networkPolicyDiscoveryServiceStreamNetworkPoliciesClient) Send(m *envoy_api_v2.DiscoveryRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *networkPolicyDiscoveryServiceStreamNetworkPoliciesClient) Recv() (*envoy_api_v2.DiscoveryResponse, error) {
	m := new(envoy_api_v2.DiscoveryResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *networkPolicyDiscoveryServiceClient) FetchNetworkPolicies(ctx context.Context, in *envoy_api_v2.DiscoveryRequest, opts ...grpc.CallOption) (*envoy_api_v2.DiscoveryResponse, error) {
	out := new(envoy_api_v2.DiscoveryResponse)
	err := grpc.Invoke(ctx, "/cilium.NetworkPolicyDiscoveryService/FetchNetworkPolicies", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for NetworkPolicyDiscoveryService service

type NetworkPolicyDiscoveryServiceServer interface {
	StreamNetworkPolicies(NetworkPolicyDiscoveryService_StreamNetworkPoliciesServer) error
	FetchNetworkPolicies(context.Context, *envoy_api_v2.DiscoveryRequest) (*envoy_api_v2.DiscoveryResponse, error)
}

func RegisterNetworkPolicyDiscoveryServiceServer(s *grpc.Server, srv NetworkPolicyDiscoveryServiceServer) {
	s.RegisterService(&_NetworkPolicyDiscoveryService_serviceDesc, srv)
}

func _NetworkPolicyDiscoveryService_StreamNetworkPolicies_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(NetworkPolicyDiscoveryServiceServer).StreamNetworkPolicies(&networkPolicyDiscoveryServiceStreamNetworkPoliciesServer{stream})
}

type NetworkPolicyDiscoveryService_StreamNetworkPoliciesServer interface {
	Send(*envoy_api_v2.DiscoveryResponse) error
	Recv() (*envoy_api_v2.DiscoveryRequest, error)
	grpc.ServerStream
}

type networkPolicyDiscoveryServiceStreamNetworkPoliciesServer struct {
	grpc.ServerStream
}

func (x *networkPolicyDiscoveryServiceStreamNetworkPoliciesServer) Send(m *envoy_api_v2.DiscoveryResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *networkPolicyDiscoveryServiceStreamNetworkPoliciesServer) Recv() (*envoy_api_v2.DiscoveryRequest, error) {
	m := new(envoy_api_v2.DiscoveryRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _NetworkPolicyDiscoveryService_FetchNetworkPolicies_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(envoy_api_v2.DiscoveryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NetworkPolicyDiscoveryServiceServer).FetchNetworkPolicies(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cilium.NetworkPolicyDiscoveryService/FetchNetworkPolicies",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NetworkPolicyDiscoveryServiceServer).FetchNetworkPolicies(ctx, req.(*envoy_api_v2.DiscoveryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _NetworkPolicyDiscoveryService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "cilium.NetworkPolicyDiscoveryService",
	HandlerType: (*NetworkPolicyDiscoveryServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FetchNetworkPolicies",
			Handler:    _NetworkPolicyDiscoveryService_FetchNetworkPolicies_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamNetworkPolicies",
			Handler:       _NetworkPolicyDiscoveryService_StreamNetworkPolicies_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "cilium/npds.proto",
}

func init() { proto.RegisterFile("cilium/npds.proto", fileDescriptor3) }

var fileDescriptor3 = []byte{
	// 553 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x54, 0xbf, 0x6e, 0xd3, 0x40,
	0x18, 0xef, 0x25, 0x69, 0x08, 0x5f, 0xd4, 0xa2, 0x9e, 0x48, 0x70, 0x23, 0x9a, 0x04, 0xb3, 0xb8,
	0x1d, 0x6c, 0xe4, 0x0c, 0x48, 0x65, 0x40, 0x44, 0x80, 0xb2, 0x80, 0x22, 0xa7, 0x13, 0x03, 0x91,
	0x6b, 0x7f, 0x4a, 0x4e, 0x75, 0x7c, 0xe6, 0x7c, 0x31, 0xca, 0x5a, 0xf1, 0x04, 0x30, 0xf1, 0x16,
	0xcc, 0x4c, 0xbc, 0x02, 0xe2, 0x15, 0x58, 0x78, 0x8a, 0x22, 0xfb, 0xec, 0xb4, 0x56, 0x13, 0xb1,
	0xb0, 0x58, 0x77, 0xfe, 0xfd, 0xf9, 0xfe, 0xdd, 0x1d, 0x1c, 0x78, 0x2c, 0x60, 0xcb, 0x85, 0x15,
	0x46, 0x7e, 0x6c, 0x46, 0x82, 0x4b, 0x4e, 0xeb, 0xea, 0x57, 0xa7, 0x87, 0x61, 0xc2, 0x57, 0x96,
	0x1b, 0x31, 0x2b, 0xb1, 0x2d, 0x8f, 0x0b, 0xb4, 0x5c, 0xdf, 0x17, 0x18, 0xe7, 0xc4, 0xce, 0xc3,
	0x12, 0xc1, 0x67, 0xb1, 0xc7, 0x13, 0x14, 0xab, 0x1c, 0xed, 0x96, 0x50, 0xc1, 0x97, 0x12, 0xd5,
	0xb7, 0x50, 0xcf, 0x38, 0x9f, 0x05, 0x98, 0x11, 0xdc, 0x30, 0xe4, 0xd2, 0x95, 0x8c, 0x87, 0x85,
	0xf7, 0x83, 0xc4, 0x0d, 0x98, 0xef, 0x4a, 0xb4, 0x8a, 0x85, 0x02, 0xf4, 0x9f, 0x04, 0xf6, 0xde,
	0xa2, 0xfc, 0xc8, 0xc5, 0xc5, 0x98, 0x07, 0xcc, 0x5b, 0xd1, 0x36, 0xd4, 0xa3, 0x6c, 0xa5, 0x91,
	0x3e, 0x31, 0x6a, 0x4e, 0xbe, 0xa3, 0x67, 0x70, 0xc8, 0xc2, 0x59, 0x9a, 0xef, 0x34, 0x42, 0x31,
	0x8d, 0xb8, 0x90, 0xd3, 0x0c, 0x62, 0x18, 0x6b, 0x95, 0x7e, 0xd5, 0x68, 0xda, 0x87, 0xa6, 0xaa,
	0xd5, 0x1c, 0x73, 0x21, 0x4b, 0xae, 0x4e, 0x3b, 0xd7, 0x8e, 0x51, 0xa4, 0xe0, 0x38, 0x17, 0x52,
	0x07, 0x34, 0xdc, 0x66, 0x5a, 0xfd, 0x97, 0x69, 0x0b, 0x37, 0x79, 0xea, 0xdf, 0x08, 0x1c, 0xdc,
	0x22, 0xd3, 0x1e, 0xd4, 0x52, 0xfb, 0xac, 0xaa, 0xbd, 0x61, 0xf3, 0xfb, 0x9f, 0x1f, 0xd5, 0xfa,
	0x49, 0x4d, 0xbb, 0xba, 0xaa, 0x3a, 0x19, 0x40, 0x5f, 0x41, 0x23, 0xeb, 0x89, 0xc7, 0x03, 0xad,
	0xd2, 0x27, 0xc6, 0xbe, 0x7d, 0x6c, 0x66, 0x4d, 0x37, 0xdd, 0x88, 0x99, 0x89, 0x6d, 0xa6, 0x33,
	0x33, 0x27, 0xdc, 0xbb, 0x40, 0xf9, 0x22, 0x9f, 0xdc, 0x38, 0x17, 0x38, 0x6b, 0x29, 0x1d, 0xc0,
	0xae, 0x58, 0x06, 0xeb, 0xf4, 0x8f, 0xb6, 0xa7, 0xbf, 0x0c, 0xd0, 0x51, 0x5c, 0xfd, 0x2b, 0x81,
	0xd6, 0x46, 0x02, 0x1d, 0xc0, 0x3d, 0x81, 0x0b, 0x2e, 0xf1, 0xba, 0x2f, 0xa4, 0x5f, 0x35, 0x6a,
	0x43, 0x48, 0x2b, 0xd8, 0xfd, 0x4c, 0x2a, 0x1a, 0x71, 0xf6, 0x15, 0x65, 0xdd, 0xd5, 0xe7, 0x00,
	0x73, 0x29, 0xa3, 0xa9, 0x4a, 0xc4, 0xef, 0x13, 0xa3, 0x69, 0x77, 0x8b, 0x44, 0x46, 0x52, 0x46,
	0xb7, 0xe2, 0xc4, 0xa3, 0x1d, 0xe7, 0x6e, 0xaa, 0xc9, 0x36, 0x43, 0x80, 0x46, 0xf0, 0x54, 0xc9,
	0xf5, 0x73, 0x68, 0x6f, 0x96, 0xd0, 0x51, 0x29, 0x0c, 0x29, 0xd7, 0xbb, 0x51, 0x73, 0x9d, 0x75,
	0x83, 0xdc, 0x88, 0xa7, 0x9f, 0x41, 0x6b, 0x23, 0x9f, 0x3e, 0x83, 0x3b, 0x73, 0x74, 0x7d, 0x14,
	0x85, 0xff, 0xa3, 0xf2, 0x4c, 0xd4, 0x15, 0x18, 0x65, 0x94, 0x37, 0xae, 0xf4, 0xe6, 0x28, 0x9c,
	0x42, 0x61, 0x7f, 0xaa, 0xc0, 0x51, 0xc9, 0xf2, 0x65, 0x71, 0xa9, 0x26, 0x28, 0x12, 0xe6, 0x21,
	0x7d, 0x0f, 0xad, 0x89, 0x14, 0xe8, 0x2e, 0x6e, 0xd2, 0xd2, 0x0e, 0x76, 0xcb, 0x61, 0xd6, 0x42,
	0x07, 0x3f, 0x2c, 0x31, 0x96, 0x9d, 0xde, 0x56, 0x3c, 0x8e, 0x78, 0x18, 0xa3, 0xbe, 0x63, 0x90,
	0x27, 0x84, 0x5e, 0x12, 0xb8, 0xff, 0x1a, 0xa5, 0x37, 0xff, 0xef, 0xfe, 0xc7, 0x97, 0xbf, 0x7e,
	0x7f, 0xa9, 0x3c, 0xd6, 0xbb, 0xa5, 0xc7, 0xe2, 0x34, 0x54, 0x71, 0xd6, 0x87, 0xe5, 0x94, 0x9c,
	0x0c, 0x1b, 0xef, 0xf2, 0x37, 0xe8, 0xbc, 0x9e, 0x9d, 0xd2, 0xc1, 0xdf, 0x00, 0x00, 0x00, 0xff,
	0xff, 0x5e, 0x11, 0xe5, 0x92, 0xa7, 0x04, 0x00, 0x00,
}
