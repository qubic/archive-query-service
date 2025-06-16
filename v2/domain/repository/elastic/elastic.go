package elastic

import (
	"github.com/elastic/go-elasticsearch/v8"
	"sync/atomic"
)

type Repository struct {
	esClient                     *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	txIndex                      string
	tickDataIndex                string
}

func NewRepository(txIndex string, tickDataIndex string, esClient *elasticsearch.Client) *Repository {
	return &Repository{
		txIndex:       txIndex,
		tickDataIndex: tickDataIndex,
		esClient:      esClient,
	}
}

//func (s *StatusCache) fetchStatusMaxTick(ctx context.Context) (uint32, error) {
//	statusResponse, err := s.StatusServiceClient.GetStatus(ctx, nil)
//	if err != nil {
//		return 0, fmt.Errorf("fetching status service: %v", err)
//	}
//
//	return statusResponse.LastProcessedTick, nil
//}
//
//func (s *StatusCache) fetchTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
//	tickIntervalsResponse, err := s.StatusServiceClient.GetTickIntervals(ctx, nil)
//	if err != nil {
//		return nil, fmt.Errorf("fetching tick intervals: %v", err)
//	}
//
//	if len(tickIntervalsResponse.Intervals) == 0 {
//		return nil, fmt.Errorf("no tick intervals found")
//	}
//
//	return tickIntervalsResponse.Intervals, nil
//}
