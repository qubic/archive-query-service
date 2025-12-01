//go:build !ci
// +build !ci

package useless

import (
	"context"
	"crypto/tls"
	"flag"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
	"github.com/qubic/archive-query-service/legacy/elastic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	elasticClient *elastic.Client
)

func TestElasticClient_QueryEmptyTicks_ReturnEmptyTicksAtBounds(t *testing.T) {
	// empty: 35340000, 35340001, 35340002, 35340065
	ticks, err := elasticClient.QueryEmptyTicks(context.Background(), 35340000, 35340065, 184)
	require.NoError(t, err)
	require.NotEmpty(t, ticks)
	assert.Len(t, ticks, 4)
	assert.Equal(t, []uint32{35340000, 35340001, 35340002, 35340065}, ticks)
}

func TestElasticClient_QueryEmptyTicks_ReturnCorrectNumberOfEmptyTicks(t *testing.T) {
	// 178000 non-empty ticks; 35524593+1-35340000=184594 ticks; 184594-178000=6594 empty ticks
	ticks, err := elasticClient.QueryEmptyTicks(context.Background(), 35340000, 35524593, 184)
	require.NoError(t, err)
	require.NotEmpty(t, ticks)
	assert.Lenf(t, ticks, 6594, "# empty ticks: %d", len(ticks))
}

func TestMain(m *testing.M) {
	setup()
	// Parse args and run
	flag.Parse()
	exitCode := m.Run()
	// Exit
	os.Exit(exitCode)
}

func setup() {
	const envPrefix = "QUBIC_LTS_QUERY_SERVICE"
	err := godotenv.Load("../../.env.local")
	if err != nil {
		log.Printf("[WARN] no env file found")
	}
	var cfg struct {
		Elastic struct {
			Addresses         []string `conf:"default:https://localhost:9200"`
			Username          string   `conf:"default:qubic-query"`
			Password          string   `conf:"optional"`
			TransactionsIndex string   `conf:"default:qubic-transactions-alias"`
			TickDataIndex     string   `conf:"default:qubic-tick-data-alias"`
			ComputorListIndex string   `conf:"default:qubic-computors-alias"`
			Certificate       string   `conf:"default:../../certs/elastic-dev/http_ca.crt"`
		}
	}
	err = conf.Parse(os.Args[1:], envPrefix, &cfg)
	if err != nil {
		log.Fatalf("error getting config: %v", err)
	}
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.Elastic.Addresses,
		Username:  cfg.Elastic.Username,
		Password:  cfg.Elastic.Password,
		Transport: &http.Transport{
			ResponseHeaderTimeout: time.Second,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		log.Fatalf("error creating elastic client: %v", err)
	}
	elasticClient = elastic.NewElasticClient(cfg.Elastic.TransactionsIndex, cfg.Elastic.TickDataIndex, cfg.Elastic.ComputorListIndex, esClient)
}
