BINARY_NAME=mcp-saga

all: test build

build:
	go build -ldflags="-linkmode=external -s -w" -o bin/$(BINARY_NAME) cmd/mcp-saga/main.go

test:
	go test -exec "" -ldflags="-linkmode=external" -v ./...

bench:
	go test -exec "" -ldflags="-linkmode=external" -v ./tests

clean:
	rm -rf bin/ *.db *.db-shm *.db-wal
