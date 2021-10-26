.PHONY: build
build:
	go build cmd/cnm-serial-api/main.go

.PHONY: test
test:
	go test -v ./...