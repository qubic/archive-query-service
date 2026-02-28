# Constants
PROTO_DIR := $(shell pwd)/api/archive-query-service/v2

# Apple arm64 specific - dynamically detect protobuf version
PROTOBUF_VERSION := $(shell ls /opt/homebrew/Cellar/protobuf/ 2>/dev/null | head -1)
OPT_ARGS := $(shell [ "$$(uname -s)" = "Darwin" ] && [ "$$(uname -m)" = "arm64" ] && [ -n "$(PROTOBUF_VERSION)" ] && echo "--proto_path=/opt/homebrew/Cellar/protobuf/$(PROTOBUF_VERSION)/include")

# Tasks
.PHONY: all mock-gen proto-gen proto-clean openapi-v3-gen test-cover

all: proto-gen mock-gen openapi-v3-gen
clean: proto-clean

mock-gen:
	@echo "Generating mocks..."
	go generate ./...

proto-gen:
	@echo "Generating protobuf files..."
	cd "$(PROTO_DIR)" && \
    protoc -I=. --go-grpc_out=paths=source_relative:. \
    		--grpc-gateway_out=allow_repeated_fields_in_body=true:. \
        --grpc-gateway_opt logtostderr=true \
        --grpc-gateway_opt paths=source_relative \
        --grpc-gateway_opt generate_unbound_methods=true $(OPT_ARGS) \
    	--go_out=paths=source_relative:. *.proto

proto-clean:
	@echo "Cleaning protobuf files..."
	cd "$(PROTO_DIR)" && \
    rm -f *.pb.go && \
    rm -f *.pb.gw.go

openapi-v3-gen:
	@echo "Generating OpenAPI v3 files..."
	cd "$(PROTO_DIR)" && \
	protoc --openapi_out=output_mode=source_relative,default_response=false:. $(OPT_ARGS) -I "$(PROTO_DIR)" \
		query_services.proto
	@echo "Cleaning up OpenAPI spec..."
	cd "$(PROTO_DIR)" && \
	yq -i 'del(.paths."/health")' query_services.openapi.yaml && \
	yq -i '(.tags[] | select(.name == "ArchiveQueryService")).x-scalar-ignore = true' query_services.openapi.yaml && \
	yq -i '(.paths[][].tags) -= ["ArchiveQueryService"]' query_services.openapi.yaml

test-cover:
	@echo "Performing Go cover test..."
	go test ./... -coverprofile=cover.out && go tool cover -html=cover.out

lint:
	@echo "Running Go linter..."
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.5.0 run --config .golangci.yml