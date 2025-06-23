# Constants
PROTO_DIR := $(shell pwd)/api/archive-query-service/v2

# Apple arm64 specific
OPT_ARGS := $(shell [[ "$$(uname -s)" == "Darwin" && "$$(uname -m)" == "arm64" ]] && echo "--proto_path=/opt/homebrew/Cellar/protobuf/25.3_1/include")

# Tasks
.PHONY: all mock-gen proto-gen proto-clean swagger-gen test-cover

all: mock-gen proto-gen swagger-gen
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
    rm -f *.go

swagger-gen:
	@echo "Generating openapi swagger files..."
	cd "$(PROTO_DIR)" && \
	protoc --openapiv2_out=logtostderr=true:. $(OPT_ARGS) \
		query_services.proto

test-cover:
	@echo "Performing Go cover test..."
	go test ./... -coverprofile=cover.out && go tool cover -html=cover.out