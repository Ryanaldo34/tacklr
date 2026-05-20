BINARY ?= tackle-server
CMD_DIR ?= ./cmd/server
BUILD_FLAGS ?= -ldflags="-s -w"

.PHONY: all build clean run test vet

all: test vet build

build:
	go build $(BUILD_FLAGS) -o bin/$(BINARY) $(CMD_DIR)

clean:
	rm -rf bin/

run: build
	./bin/$(BINARY)

test:
	go test -race -count=1 ./...

vet:
	go vet ./...
