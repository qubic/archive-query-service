package main

import (
	"errors"
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/qubic/archive-query-service/rpc"
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
			ReadTimeout     time.Duration `conf:"default:5s"`
			WriteTimeout    time.Duration `conf:"default:5s"`
			ShutdownTimeout time.Duration `conf:"default:5s"`
			HttpHost        string        `conf:"default:0.0.0.0:8000"`
			GrpcHost        string        `conf:"default:0.0.0.0:8001"`
			ProfilingHost   string        `conf:"default:0.0.0.0:8002"`
		}
		ElasticSearch struct {
			Address     string        `conf:"default:http://127.0.0.1:9200"`
			ReadTimeout time.Duration `conf:"default:10s"`
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

	elsCfg := elasticsearch.Config{
		Addresses: []string{cfg.ElasticSearch.Address},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: cfg.ElasticSearch.ReadTimeout,
		},
	}

	esClient, err := elasticsearch.NewClient(elsCfg)
	if err != nil {
		return fmt.Errorf("creating elasticsearch client: %v", err)
	}

	rpcServer := rpc.NewServer(cfg.Server.GrpcHost, cfg.Server.HttpHost, esClient)
	err = rpcServer.Start()
	if err != nil {
		return fmt.Errorf("starting rpc server: %v", err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	pprofErrors := make(chan error, 1)

	go func() {
		pprofErrors <- http.ListenAndServe(cfg.Server.ProfilingHost, nil)
	}()

	for {
		select {
		case <-shutdown:
			return errors.New("shutting down")
		case err := <-pprofErrors:
			return fmt.Errorf("pprof error: %v", err)
		}
	}

	return nil
}
