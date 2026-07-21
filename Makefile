.PHONY: test vet

test:
	go test -race -count=1 ./...

vet:
	go vet ./...
