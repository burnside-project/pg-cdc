.PHONY: build lint test clean

BINARY := pg-cdc
VERSION := $(shell cat VERSION | tr -d '[:space:]')
LDFLAGS := -s -w -X github.com/burnside-project/pg-cdc-core/pkg/version.Version=v$(VERSION)-dev

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/pg-cdc/

lint:
	golangci-lint run ./...

test:
	go test -race -count=1 -timeout 120s ./...

vet:
	go vet ./...

clean:
	rm -f $(BINARY)

# Cross-compile all platforms (pure Go, no CGO needed)
release:
	@for pair in linux/amd64 linux/arm64 darwin/arm64 windows/amd64; do \
		GOOS=$${pair%/*} GOARCH=$${pair#*/} CGO_ENABLED=0 \
		go build -trimpath -ldflags "$(LDFLAGS)" \
		-o "$(BINARY)-$${pair%/*}-$${pair#*/}" ./cmd/pg-cdc/; \
	done

all: vet lint test build
