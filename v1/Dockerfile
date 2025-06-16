FROM golang:1.24 AS builder
ENV CGO_ENABLED=0

WORKDIR /src
COPY . /src

RUN go build -o "./bin/archive-query-service" "./app/archive-query-service"

# We don't need golang to run binaries, just use alpine.
FROM alpine
COPY --from=builder /src/bin/archive-query-service /app/archive-query-service

EXPOSE 8000
EXPOSE 8001
EXPOSE 8002

WORKDIR /app

ENTRYPOINT ["./archive-query-service"]
