package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/archive-query-service/legacy/elastic"
	"github.com/qubic/archive-query-service/legacy/rpc"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const prefix = "QUBIC_LTS_QUERY_SERVICE"

func main() {
	log.SetOutput(os.Stdout)
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
			StatusServiceGrpcHost string        `conf:"default:127.0.0.1:9901"`
			StatusDataCacheTtl    time.Duration `conf:"default:1s"`
			EmptyTicksTtl         time.Duration `conf:"default:24h"`
		}
		Pagination struct {
			EnforcePageLimits bool  `conf:"default:true"`
			AllowedPageSizes  []int `conf:"default:10;25;50;100"`
			DefaultPageSize   int   `conf:"default:10"`
			MaxAllowedOffset  int   `conf:"default:10000"`
		}
		ElasticSearch struct {
			Address                               []string      `conf:"default:https://localhost:9200"`
			Username                              string        `conf:"default:qubic-query"`
			Password                              string        `conf:"mask,optional"`
			CertificatePath                       string        `conf:"default:http_ca.crt"`
			MaxRetries                            int           `conf:"default:3"`
			ReadTimeout                           time.Duration `conf:"default:10s"`
			ConsecutiveRequestErrorCountThreshold int           `conf:"default:10"`
			TransactionsIndex                     string        `conf:"default:qubic-transactions-alias"`
			TickDataIndex                         string        `conf:"default:qubic-tick-data-alias"`
			ComputorListIndex                     string        `conf:"default:qubic-computors-alias"`
		}
		Metrics struct {
			Namespace string `conf:"default:query_service_v1"`
			Port      int    `conf:"default:9999"`
		}
	}

	if err := conf.Parse(os.Args[1:], prefix, &cfg); err != nil {
		switch {
		case errors.Is(err, conf.ErrHelpWanted):
			usage, err := conf.Usage(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config usage: %w", err)
			}
			fmt.Println(usage)
			return nil
		case errors.Is(err, conf.ErrVersionWanted):
			version, err := conf.VersionString(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config version: %w", err)
			}
			fmt.Println(version)
			return nil
		}
		return fmt.Errorf("parsing config: %w", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %w", err)
	}
	log.Printf("main: Config :\n%v\n", out)

	cert, err := os.ReadFile(cfg.ElasticSearch.CertificatePath)
	if err != nil {
		log.Printf("warn: Failed to load Elastic certificate file: %v\n", err)
	}

	elsCfg := elasticsearch.Config{
		Addresses:     cfg.ElasticSearch.Address,
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
		return fmt.Errorf("creating elasticsearch client: %w", err)
	}
	elasticClient := elastic.NewElasticClient(cfg.ElasticSearch.TransactionsIndex, cfg.ElasticSearch.TickDataIndex, cfg.ElasticSearch.ComputorListIndex, esClient)

	srvMetrics := grpcProm.NewServerMetrics(
		grpcProm.WithServerCounterOptions(grpcProm.WithConstLabels(prometheus.Labels{"namespace": cfg.Metrics.Namespace})),
	)
	reg := prometheus.DefaultRegisterer
	reg.MustRegister(srvMetrics)

	statusServiceGrpcConn, err := grpc.NewClient(cfg.Server.StatusServiceGrpcHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("creating archiver api connection: %w", err)
	}
	statusServiceClient := statusPb.NewStatusServiceClient(statusServiceGrpcConn)

	cache := rpc.NewStatusCache(statusServiceClient, cfg.Server.EmptyTicksTtl, cfg.Server.StatusDataCacheTtl)

	go cache.Start()
	defer cache.Stop()

	queryService := rpc.NewQueryService(cfg.ElasticSearch.TransactionsIndex, cfg.ElasticSearch.TickDataIndex, cfg.ElasticSearch.ComputorListIndex, elasticClient, cache)
	paginationLimits := rpc.NewPaginationLimits(cfg.Pagination.EnforcePageLimits, cfg.Pagination.AllowedPageSizes, cfg.Pagination.DefaultPageSize, cfg.Pagination.MaxAllowedOffset)
	rpcServer := rpc.NewServer(cfg.Server.GrpcHost, cfg.Server.HttpHost, queryService, statusServiceClient, paginationLimits)
	tickInBoundsInterceptor := rpc.NewTickWithinBoundsInterceptor(statusServiceClient, cache)
	var identitiesValidatorInterceptor rpc.IdentitiesValidatorInterceptor
	var logTechnicalErrorInterceptor rpc.LogTechnicalErrorInterceptor
	err = rpcServer.Start(srvMetrics.UnaryServerInterceptor(),
		logTechnicalErrorInterceptor.GetInterceptor,
		tickInBoundsInterceptor.GetInterceptor,
		identitiesValidatorInterceptor.GetInterceptor)
	if err != nil {
		return fmt.Errorf("starting rpc server: %w", err)
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

			consecutiveErrorCount := int(queryService.ConsecutiveElasticErrorCount.Load())

			if consecutiveErrorCount >= cfg.ElasticSearch.ConsecutiveRequestErrorCountThreshold {
				writer.WriteHeader(http.StatusInternalServerError)
			}
			_, err := writer.Write([]byte{})
			if err != nil {
				log.Println("failed to respond to status request")
			}

		})

		http.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}))
		webServerErr <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Metrics.Port), nil)
	}()

	for {
		select {
		case <-shutdown:
			return errors.New("shutting down")
		case err := <-pprofErrors:
			return fmt.Errorf("pprof error: %w", err)
		case err := <-webServerErr:
			return fmt.Errorf("web server error: %w", err)

		}
	}
}
