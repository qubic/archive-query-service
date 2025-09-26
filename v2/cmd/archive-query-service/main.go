package main

import (
	"errors"
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/archive-query-service/v2/domain"
	"github.com/qubic/archive-query-service/v2/domain/repository/elastic"
	rpc "github.com/qubic/archive-query-service/v2/grpc"
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

const prefix = "QUBIC_LTS_QUERY_SERVICE_V2"

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
			HttpHost              string        `conf:"default:0.0.0.0:8000"` //nolint:revive
			GrpcHost              string        `conf:"default:0.0.0.0:8001"`
			ProfilingHost         string        `conf:"default:0.0.0.0:8002"`
			StatusServiceGrpcHost string        `conf:"default:localhost:9901"`
			StatusDataCacheTTL    time.Duration `conf:"default:1s"`
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
			ComputorsListIndex                    string        `conf:"default:qubic-computors-alias"`
		}
		Metrics struct {
			Namespace string `conf:"default:query-service-v2"`
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

	cache := domain.NewStatusGetter(statusServiceClient, cfg.Server.StatusDataCacheTTL)

	go cache.Start()
	defer cache.Stop()

	repo := elastic.NewRepository(cfg.ElasticSearch.TransactionsIndex, cfg.ElasticSearch.TickDataIndex, cfg.ElasticSearch.ComputorsListIndex, esClient)

	txService := domain.NewTransactionService(repo, cache.GetMaxTick)
	tdService := domain.NewTickDataService(repo)
	statusService := domain.NewStatusService(cache)
	clService := domain.NewComputorsListService(repo)
	rpcServer := rpc.NewArchiveQueryService(txService, tdService, statusService, clService)
	tickInBoundsInterceptor := rpc.NewTickWithinBoundsInterceptor(statusService)
	var identitiesValidatorInterceptor rpc.IdentitiesValidatorInterceptor
	var logTechnicalErrorInterceptor rpc.LogTechnicalErrorInterceptor
	startCfg := rpc.StartConfig{
		ListenAddrGRPC: cfg.Server.GrpcHost,
		ListenAddrHTTP: cfg.Server.HttpHost,
	}

	srvErrorsChan := make(chan error, 1)
	err = rpcServer.Start(startCfg, srvErrorsChan, srvMetrics.UnaryServerInterceptor(),
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
		pprofErrors <- http.ListenAndServe(cfg.Server.ProfilingHost, nil) //nolint:gosec
	}()

	webServerErr := make(chan error, 1)
	go func() {
		log.Printf("main: Starting status and metrics endpoints on port [%d]\n", cfg.Metrics.Port)
		http.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}))
		webServerErr <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Metrics.Port), nil) //nolint:gosec
	}()

	for {
		select {
		case <-shutdown:
			return errors.New("shutting down")
		case err := <-pprofErrors:
			return fmt.Errorf("pprof error: %w", err)
		case err := <-webServerErr:
			return fmt.Errorf("web server error: %w", err)
		case err := <-srvErrorsChan:
			return fmt.Errorf("grpc server error: %w", err)

		}
	}
}
