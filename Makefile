export DATABASE_URL ?= postgres://oversell:oversell@localhost:54329/oversell?sslmode=disable
export REDIS_URL    ?= redis://localhost:63799/0

.PHONY: up down test bench server lint

up:      ## start Postgres and Redis
	docker compose up -d --wait

down:    ## stop and remove containers
	docker compose down -v

test:    ## run unit + integration tests with the race detector
	TEST_DATABASE_URL=$(DATABASE_URL) TEST_REDIS_URL=$(REDIS_URL) go test -race -count=1 ./...

bench:   ## run every strategy against the same flash sale
	go run ./cmd/bench -strategy all

server:  ## start the HTTP API on :8080
	go run ./cmd/server

lint:
	go vet ./...
