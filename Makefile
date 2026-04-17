# Makefile for Metri Engine (Clojure)

.PHONY: up down repl build build-uberjar test-all check-cedar clean

up:
	@echo "Starting local Valkey container..."
	docker-compose up -d

down:
	@echo "Stopping local Valkey container..."
	docker-compose down

repl:
	@echo "Starting Clojure REPL with metri-dev profile via Docker..."
	docker run -it --rm -v $(PWD):/app -w /app clojure:temurin-21-tools-deps sh -c "AWS_PROFILE=metri-dev set -a && source .env && set +a && clj -M:dev"

build:
	@echo "Enforcing AOT constraints & building GraalVM native artifact..."
	docker run --rm -v $(PWD):/app -w /app clojure:temurin-21-tools-deps clj -T:build native

build-uberjar:
	@echo "Building generic uberjar via Docker (AWS SnapStart approach)..."
	docker run --rm -v $(PWD):/app -v ~/.m2:/root/.m2 -w /app clojure:temurin-21-tools-deps clj -T:build uber

test-all:
	@echo "Running all tests via Docker..."
	docker run --rm -v $(PWD):/app -w /app clojure:temurin-21-tools-deps sh -c "AWS_PROFILE=metri-dev set -a && source .env && set +a && clj -M:test"

check-cedar:
	@echo "Validating AVP policies syntax..."
	@if command -v cedar-policy-cli > /dev/null; then \
		cedar-policy-cli validate -p policies.cedar -s schema.cedar; \
	else \
		echo "Warning: cedar-policy-cli not installed locally, skipping local syntax check."; \
	fi

clean:
	@echo "Cleaning target directory..."
	rm -rf target/
