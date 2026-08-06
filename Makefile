.PHONY: build build-server build-cli web test test-unit test-integration test-e2e lint vet fmt tidy install-bins docs clean

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/krply/krply/internal/version.Version=$(VERSION)

build: build-cli build-server

build-cli:
	go build -ldflags '$(LDFLAGS)' -o bin/krply ./cmd/krply

build-server:
	go build -ldflags '$(LDFLAGS)' -o bin/krply-server ./cmd/krply-server

web:
	cd web && npm install && npm run build

test:
	go test ./...

test-unit:
	go test ./internal/... ./cmd/... -short

test-integration:
	go test ./test/integration/... -tags integration -timeout 20m

test-e2e:
	go test ./test/e2e/... -tags e2e -timeout 30m

lint:
	go vet ./...
	cd web && npm run lint 2>/dev/null || true

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

tidy:
	go mod tidy

install-bins: build
	install -m 0755 bin/krply /usr/local/bin/krply
	install -m 0755 bin/krply-server /usr/local/bin/krply-server

docs:
	@echo "documentation lives in docs/"

clean:
	rm -rf bin web/dist
