BINARY := switchyard
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build run test race cover lint fmt docker

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/switchyard

run: build
	./bin/$(BINARY) -config config.example.yaml

test:
	go test ./...

race:
	go test -race -count=1 ./...

cover:
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

lint:
	go vet ./...
	golangci-lint run

fmt:
	gofmt -s -w .

docker:
	docker build -t $(BINARY):$(VERSION) .
