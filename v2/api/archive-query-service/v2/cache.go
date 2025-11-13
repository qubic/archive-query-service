package api

import (
	"strconv"
	"time"
)

func (r *GetTickDataRequest) GetCacheKey() string {
	return "gtdr:" + strconv.FormatUint(uint64(r.TickNumber), 10)
}

func (r *GetTickDataRequest) GetTTL() time.Duration {
	return 1 * time.Second
}
