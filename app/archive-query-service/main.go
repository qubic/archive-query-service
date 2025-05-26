package main

import (
	"errors"
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/archive-query-service/rpc"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const prefix = "QUBIC_LTS_QUERY_SERVICE"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	var cfg struct {
		Server struct {
			ReadTimeout           time.Duration `conf:"default:5s"`
			WriteTimeout          time.Duration `conf:"default:5s"`
			ShutdownTimeout       time.Duration `conf:"default:5s"`
			HttpHost              string        `conf:"default:0.0.0.0:8000"`
			GrpcHost              string        `conf:"default:0.0.0.0:8001"`
			ProfilingHost         string        `conf:"default:0.0.0.0:8002"`
			StatusServiceGrpcHost string        `conf:"default:127.0.0.0:9901"`
		}
		ElasticSearch struct {
			Address                               string        `conf:"default:http://127.0.0.1:9200"`
			Username                              string        `conf:"default:qubic-query"`
			Password                              string        `conf:"optional"`
			CertificatePath                       string        `conf:"default:http_ca.crt"`
			MaxRetries                            int           `conf:"default:10"`
			ReadTimeout                           time.Duration `conf:"default:10s"`
			ConsecutiveRequestErrorCountThreshold int           `conf:"default:10"`
		}
		Metrics struct {
			Namespace string `conf:"default:qubic-query"`
			Port      int    `conf:"default:9999"`
		}
	}

	if err := conf.Parse(os.Args[1:], prefix, &cfg); err != nil {
		switch {
		case errors.Is(err, conf.ErrHelpWanted):
			usage, err := conf.Usage(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config usage: %v", err)
			}
			fmt.Println(usage)
			return nil
		case errors.Is(err, conf.ErrVersionWanted):
			version, err := conf.VersionString(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config version: %v", err)
			}
			fmt.Println(version)
			return nil
		}
		return fmt.Errorf("parsing config: %v", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %v", err)
	}
	log.Printf("main: Config :\n%v\n", out)

	cert, err := os.ReadFile(cfg.ElasticSearch.CertificatePath)
	if err != nil {
		log.Printf("info: Failed to load Elastic certificate file: %v\n", err)
	}

	elsCfg := elasticsearch.Config{
		Addresses:     []string{cfg.ElasticSearch.Address},
		Username:      cfg.ElasticSearch.Username,
		Password:      cfg.ElasticSearch.Password,
		CACert:        cert,
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    cfg.ElasticSearch.MaxRetries,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: cfg.ElasticSearch.ReadTimeout,
		},
	}

	esClient, err := elasticsearch.NewClient(elsCfg)
	if err != nil {
		return fmt.Errorf("creating elasticsearch client: %v", err)
	}

	cache := ttlcache.New[string, uint32](
		ttlcache.WithTTL[string, uint32](time.Second),
		ttlcache.WithDisableTouchOnHit[string, uint32](), // don't refresh ttl upon getting the item from cache
	)

	go cache.Start()
	defer cache.Stop()

	srvMetrics := grpcProm.NewServerMetrics(
		grpcProm.WithServerCounterOptions(grpcProm.WithConstLabels(prometheus.Labels{"namespace": "query-service"})),
	)
	reg := prometheus.NewRegistry()
	reg.MustRegister(srvMetrics)
	reg.MustRegister(collectors.NewGoCollector())

	statusServiceGrpcConn, err := grpc.NewClient(cfg.Server.StatusServiceGrpcHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("creating archiver api connection: %v", err)
	}
	statusServiceClient := statusPb.NewStatusServiceClient(statusServiceGrpcConn)

	queryBuilder := rpc.NewQueryBuilder(esClient, statusServiceClient, cache)
	rpcServer := rpc.NewServer(cfg.Server.GrpcHost, cfg.Server.HttpHost, queryBuilder)
	tickInBoundsInterceptor := rpc.NewTickWithinBoundsInterceptor(statusServiceClient)
	err = rpcServer.Start(srvMetrics.UnaryServerInterceptor(), tickInBoundsInterceptor.GetInterceptor)
	if err != nil {
		return fmt.Errorf("starting rpc server: %v", err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	pprofErrors := make(chan error, 1)

	go func() {
		pprofErrors <- http.ListenAndServe(cfg.Server.ProfilingHost, nil)
	}()

	webServerErr := make(chan error, 1)
	go func() {
		log.Printf("main: Starting status and metrics endpoints on port [%d]\n", cfg.Metrics.Port)

		http.HandleFunc("/v1/status", func(writer http.ResponseWriter, request *http.Request) {

			consecutiveErrorCount := int(queryBuilder.ConsecutiveElasticErrorCount.Load())

			if consecutiveErrorCount >= cfg.ElasticSearch.ConsecutiveRequestErrorCountThreshold {
				writer.WriteHeader(http.StatusInternalServerError)
			}
			_, err := writer.Write([]byte{})
			if err != nil {
				log.Println("failed to respond to status request")
			}

		})

		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}))
		webServerErr <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Metrics.Port), nil)
	}()

	for {
		select {
		case <-shutdown:
			return errors.New("shutting down")
		case err := <-pprofErrors:
			return fmt.Errorf("pprof error: %v", err)
		case err := <-webServerErr:
			return fmt.Errorf("web server error: %v", err)

		}
	}
}
