# Redis Caching Integration Guide

## Overview

This document describes the integration of the `RedisCacheInterceptor` in the gRPC service, including cache key management, configuration, enabling/disabling caching, and requirements for deployment.

## Table of Contents

- [Overview](#overview)
- [Interceptor Concept and Flow](#interceptor-concept-and-flow)
    - [Interceptor Flow](#interceptor-flow)
    - [Example Flow](#example-flow)
- [Cache Key Construction](#cache-key-construction)
- [Adding a New Cacheable Request](#adding-a-new-cacheable-request)
    - [Deterministic Cache Key Construction](#deterministic-cache-key-construction)
- [Serialization and Deserialization](#serialization-and-deserialization)
    - [Why we use `anypb.Any` in the interceptor](#why-we-use-anypbany-in-the-interceptor)
- [Criteria for Caching Endpoints](#criteria-for-caching-endpoints)
- [Cache Invalidation and Updates](#cache-invalidation-and-updates)
- [Applying Caching to New Endpoints](#applying-caching-to-new-endpoints)
- [Examples](#examples)
    - [Cacheable Request Example](#cacheable-request-example)
    - [TTL Map JSON Example](#ttl-map-json-example)
- [How to enable caching globally](#how-to-enable-caching-globally)
    - [Required Configuration When Caching is Enabled](#required-configuration-when-caching-is-enabled)
- [Initialisation flow in `main.go` when caching is enabled](#initialisation-flow-in-maingo-when-caching-is-enabled)
- [Docker usage](#docker-usage)

---

## Interceptor Concept and Flow

A **gRPC** interceptor acts as a middleware, processing requests and responses before they reach the handler. The `RedisCacheInterceptor` transparently caches responses for eligible requests, improving performance and reducing backend load.

### Interceptor Flow

1. **Check if Request is Cacheable**  
   The interceptor checks if the request implements the `Cacheable` interface (i.e., has a `GetCacheKey()` method).

2. **Retrieve TTL from Map**  
   The gRPC method name is looked up in a TTL map (`map[string]time.Duration`). If not present or TTL is zero, caching is skipped.

3. **Generate Cache Key**  
   The request's `GetCacheKey()` method generates a unique, deterministic cache key based on request parameters.

4. **Set Cache-Control Header**  
   A `cache-control` header is set with the TTL value.

5. **Singleflight Deduplication**  
   Uses a `singleflight.Group` to ensure only one request for a given cache key is in-flight at a time.

6. **Cache Lookup and Population**
    - If a cached response exists in Redis, it is returned.
    - Otherwise, the handler is called and the response is cached for the configured TTL.

7. **Serialization/Deserialization**  
   Responses are serialized using protobuf's `anypb.Any` and stored as bytes in Redis. On retrieval, they are deserialized back into the proto message.

#### Example Flow

```go
// Pseudocode for the flow
if req implements Cacheable {
    if ttlMap[method] > 0 {
        key := req.GetCacheKey()
        if cached, found := redis.Get(key); found {
            return cached
        }
        resp := handler(ctx, req)
        redis.Set(key, resp, ttl)
        return resp
    }
}
return handler(ctx, req)
```

#### Error handling
- If the `redis` connection is for any reason unavailable, caching logic is ignored and the request is processed normally.

## Cache Key Construction

Cache keys uniquely and deterministically identify a request, ensuring requests with the same parameters always map to the same cache entry.

For simple requests like `GetTickDataRequest` use straightforward keys based on request fields:
```
tdr:<tick_number>
```

For complex requests like `GetTransactionsForIdentityRequest`, use deterministic protobuf serialization combined with SHA-256 hashing to ensure uniqueness and handle nested structures. For example:
```
ttfir:<sha256(proto.Marshal(request, deterministic=True))>
```

## Adding a New Cacheable Request

1. Implement the `Cacheable` interface by adding a `GetCacheKey()` method.
2. Choose a unique prefix for the cache key. We used acronyms of the request type names (e.g., `tdr` for `GetTickDataRequest`).
3. For complex requests, use deterministic protobuf marshaling and hashing.

### Example:
```go
func (r *MyRequest) GetCacheKey() (string, error) {
    b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
    if err != nil {
        return "", err
    }
    sum := sha256.Sum256(b)
    return "mr:" + hex.EncodeToString(sum[:]), nil
}
```

### Deterministic Cache Key Construction

- Use proto.MarshalOptions{Deterministic: true} to serialize the request, then hash the bytes.
- Ensures requests with the same logical parameters (even if map order differs) produce the same cache key.

## Serialization and Deserialization

- For serialization request data is wrapped in `anypb.Any` and marshaled to bytes before storing in Redis.
- For deserialization on cache hit, bytes are unmarshaled into `anypb.Any`, then into the original proto message.

### Why we use `anypb.Any` in the interceptor

We use `anypb.Any` so the interceptor can handle **any gRPC request or response type** without knowing the concrete protobuf types in advance. `Any` wraps messages with both their serialized bytes and their type information, which gives us:

1. **Type-agnostic handling** — the interceptor can serialize and inspect any message using a single code path.
2. **Safe deserialization** — `Any` includes the `type_url`, so we always know the original protobuf type when unpacking.
3. **Deterministic serialization for caching** — useful for building stable cache keys or storing payloads.
4. **Service-independent interceptor logic** — no imports or dependencies on specific message types.

In short: **`anypb.Any` lets the interceptor generically process all protobuf messages while preserving their exact types.**

## Criteria for Caching Endpoints
- Endpoint must be idempotent and safe to cache (e.g., read-only queries).
- Request type must implement the Cacheable interface.
- Endpoint must have a non-zero TTL configured in the TTL map.

## Cache Invalidation and Updates
- **Automatic Expiry**:
Cache entries expire automatically based on their TTL.
- **Parameter Changes**:
Any change in parameters results in a new cache key.
- **Manual Invalidation**:
Cache entries can be deleted from Redis using their cache key. For example if you want to invalidate the cache for a tick data request with the tick number `37920918` you can use `redis-cli`:
```bash
$: redis-cli
127.0.0.1:6379> keys *
1) "tdr:37920918"
127.0.0.1:6379> del tdr:37920918
(integer) 1
127.0.0.1:6379> keys *
(empty array)
127.0.0.1:6379>
```

## Applying Caching to New Endpoints
1. Implement `Cacheable` for the request type.
2. Add the gRPC method name and desired TTL to the TTL map (JSON file).
3. Ensure the interceptor is registered in the gRPC server middleware chain.

## Examples

### Cacheable Request Example:
```go
// GetTickDataRequest is already generated by protobuf so we just need to add the method to it.
func (r *GetTickDataRequest) GetCacheKey() (string, error) {
    return "tdr:" + strconv.FormatUint(uint64(r.TickNumber), 10), nil
}
```

### TTL Map JSON Example:
```json
{
  "/qubic.v2.archive.pb.ArchiveQueryService/GetTickData": "60s"
}
```

## How to enable caching globally

By default, caching is disabled. To enable it, set the following environment variable:
```
QUBIC_LTS_QUERY_SERVICE_V2_SERVER_CACHE_ENABLED=true
```

### Required Configuration When Caching is Enabled
1. Cache TTL File where path value is set via the following environment variable:
```
QUBIC_LTS_QUERY_SERVICE_V2_SERVER_CACHE_TTL_FILE="cache_ttl.json"
```
2. Redis connection details via the following environment variables:
```
QUBIC_LTS_QUERY_SERVICE_V2_REDIS_ADDRESS="localhost:6379"
QUBIC_LTS_QUERY_SERVICE_V2_REDIS_PASSWORD="password"
QUBIC_LTS_QUERY_SERVICE_V2_REDIS_DB="0"
```

## Initialisation flow in `main.go` when caching is enabled

1. The TTL map is loaded from the configured JSON file.
2. A Redis client is created using the provided connection info.
3. The service checks connectivity to `Redis` by calling the `Ping` method.
4. The `RedisCacheInterceptor` is initialized and added to the gRPC middleware chain.


## Docker usage

```yaml
environment:
  - QUBIC_LTS_QUERY_SERVICE_V2_SERVER_CACHE_ENABLED=true
  - QUBIC_LTS_QUERY_SERVICE_V2_SERVER_CACHE_TTL_FILE=/config/cache_ttl.json
  - QUBIC_LTS_QUERY_SERVICE_V2_REDIS_ADDRESS=redis:6379
  - QUBIC_LTS_QUERY_SERVICE_V2_REDIS_PASSWORD=your_redis_password
  - QUBIC_LTS_QUERY_SERVICE_V2_REDIS_DB=0
volumes:
  - ./cache_ttl.json:/config/cache_ttl.json
```




