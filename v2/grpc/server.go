package grpc

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	"net"
	"net/http"
)

type StartConfig struct {
	ListenAddrGRPC string
	ListenAddrHTTP string
}

func (s *ArchiveQueryService) Start(cfg StartConfig, errCh chan error, interceptors ...grpc.UnaryServerInterceptor) error {

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(600*1024*1024),
		grpc.MaxSendMsgSize(600*1024*1024),
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
					grpc.MaxCallRecvMsgSize(600*1024*1024),
					grpc.MaxCallSendMsgSize(600*1024*1024),
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

			if err := http.ListenAndServe(cfg.ListenAddrHTTP, mux); err != nil {
				errCh <- fmt.Errorf("listening in http port: %w", err)
				return
			}
		}()
	}

	return nil
}
