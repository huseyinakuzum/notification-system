.PHONY: build test test-integration lint tidy up down migrate topics e2e

build:
	go build ./...

test:
	go test ./... -race -count=1

test-integration:
	go test ./... -tags=integration -race -count=1

lint:
	golangci-lint run

tidy:
	go mod tidy

up:
	podman compose up -d --build

down:
	podman compose down -v

migrate:
	podman compose run --rm migrate

topics:
	podman compose run --rm topics-init

e2e:
	go test ./... -tags=e2e -count=1
