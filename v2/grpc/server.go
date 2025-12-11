package grpc

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
)

type StartConfig struct {
	ListenAddrGRPC string
	ListenAddrHTTP string
	MaxRecvMsgSize int // limit receive size (request)
	MaxSendMsgSize int // limit send size (response)
}

func (s *ArchiveQueryService) Start(cfg StartConfig, errCh chan error, interceptors ...grpc.UnaryServerInterceptor) error {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.MaxSendMsgSize),
		grpc.ChainUnaryInterceptor(interceptors...),
	)
	api.RegisterArchiveQueryServiceServer(srv, s)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", cfg.ListenAddrGRPC)
	if err != nil {
		return fmt.Errorf("listening on grpc port: %w", err)
	}

	go func() {
		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}()

	if cfg.ListenAddrHTTP != "" {
		go func() {
			mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
				MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: true},
			}))
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize),
					grpc.MaxCallSendMsgSize(cfg.MaxSendMsgSize),
				),
			}

			if err := api.RegisterArchiveQueryServiceHandlerFromEndpoint(
				context.Background(),
				mux,
				cfg.ListenAddrGRPC,
				opts,
			); err != nil {
				errCh <- fmt.Errorf("registering http handler: %w", err)
				return
			}

			if err := http.ListenAndServe(cfg.ListenAddrHTTP, mux); err != nil { // nolint: gosec
				errCh <- fmt.Errorf("listening in http port: %w", err)
				return
			}
		}()
	}

	s.srv = srv
	s.grpcListenAddr = lis.Addr()

	return nil
}

func (s *ArchiveQueryService) Stop() {
	if s.srv != nil {
		s.srv.GracefulStop()
	}
}

func (s *ArchiveQueryService) GetGRPCListenAddr() net.Addr {
	return s.grpcListenAddr
}
