package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"
)

const (
	getTickDataRequestPrefix         = "tdr"
	getTransactionsForTickPrefix     = "ttfr"
	getTransactionsForIdentityPrefix = "ttfir"
	getEventsRequestPrefix           = "ger"
)

func (r *GetTickDataRequest) GetCacheKey() (string, error) {
	return getTickDataRequestPrefix + ":" + strconv.FormatUint(uint64(r.TickNumber), 10), nil
}

func (r *GetTransactionsForTickRequest) GetCacheKey() (string, error) {
	// With filters/ranges, use hash of deterministic protobuf marshal
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("marshalling request: %w", err)
	}

	sum := sha256.Sum256(b)
	return getTransactionsForTickPrefix + ":" + hex.EncodeToString(sum[:]), nil
}

func (r *GetTransactionsForIdentityRequest) GetCacheKey() (string, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("marshalling request: %w", err)
	}

	// hash the bytes
	sum := sha256.Sum256(b)

	return getTransactionsForIdentityPrefix + ":" + hex.EncodeToString(sum[:]), nil
}

func (r *GetEventsRequest) GetCacheKey() (string, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("marshalling request: %w", err)
	}

	sum := sha256.Sum256(b)
	return getEventsRequestPrefix + ":" + hex.EncodeToString(sum[:]), nil
}
