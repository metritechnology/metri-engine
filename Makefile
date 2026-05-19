.PHONY: help infra infra-down engine dev watch build build-release test check \
        seed smoke deploy clean fmt lint

# ══════════════════════════════════════════════════════════════════════════════
#  Metri Engine — Makefile (Rust Native)
# ══════════════════════════════════════════════════════════════════════════════

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Infraestructura local (sin engine) ───────────────────────────────────────

infra: ## Levanta DynamoDB Local + MinIO + ElasticMQ
	@echo "▶ Levantando infraestructura AWS local..."
	docker compose up -d dynamodb-local dynamodb-init minio minio-init elasticmq
	@./scripts/local/wait-infra.sh

infra-down: ## Detiene y limpia la infraestructura
	docker compose down -v
	@echo "✅ Stack detenido."

# ── Servidor gRPC Rust ───────────────────────────────────────────────────────

engine: infra ## Levanta infra + servidor gRPC Rust compilado
	@echo "▶ Levantando Metri Engine Rust (profile=engine)..."
	docker compose --profile engine up -d
	@echo "✅ gRPC escuchando en localhost:9090"
	@echo "   grpcurl -plaintext localhost:9090 list"

dev: infra ## Levanta infra + hot-reload (cargo-watch, profile=dev)
	@echo "▶ Modo dev con cargo-watch (hot-reload en src/)..."
	docker compose --profile dev up

watch: ## Solo hot-reload (sin levantar infra de nuevo)
	docker compose --profile dev up engine-watch

# ── Compilación Rust local ───────────────────────────────────────────────────

build: ## Compila el binario en modo debug
	cargo build

build-release: ## Compila el binario en modo release (idéntico a Lambda)
	cargo build --release

fmt: ## Formatea el código con rustfmt
	cargo fmt

lint: ## Ejecuta clippy
	cargo clippy -- -D warnings

check: ## cargo check rápido
	cargo check

# ── Tests ────────────────────────────────────────────────────────────────────

test: ## Ejecuta todos los tests unitarios
	cargo test

test-integration: infra ## Tests de integración contra DynamoDB Local
	DYNAMODB_ENDPOINT=http://localhost:8000 \
	EAV_TABLE=metri-eav-local \
	SCHEMAS_TABLE=metri-schemas-local \
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	AWS_DEFAULT_REGION=us-east-1 \
	cargo test --test '*' -- --ignored

# ── Seed ─────────────────────────────────────────────────────────────────────

seed: ## Inserta datos de prueba (tenant demo + work_orders)
	@./scripts/local/seed.sh

smoke: ## Smoke test gRPC contra el engine local
	@./scripts/local/grpcurl-test.sh

# ── Docker image ──────────────────────────────────────────────────────────────

docker-build: ## Compila la imagen Docker del engine (release)
	docker compose build engine

# ── Deploy AWS ───────────────────────────────────────────────────────────────

deploy: build-release ## Build + deploy SAM a AWS
	@echo "▶ Desplegando a AWS..."
	sam build
	sam deploy --no-confirm-changeset --profile metri-dev

# ── SAM Build target (invocado por sam build) ────────────────────────────────
build-lambda:
	cargo lambda build --release --arm64

build-MetriEngineFunction: build-lambda
	mkdir -p $(ARTIFACTS_DIR)/lib
	cp target/lambda/bootstrap/bootstrap $(ARTIFACTS_DIR)/
	cp -r config/models $(ARTIFACTS_DIR)/models
	cp -r config/errors $(ARTIFACTS_DIR)/errors

# ── Limpieza ──────────────────────────────────────────────────────────────────

clean: ## Limpia binarios compilados
	cargo clean
	@echo "✅ target/ limpiado."

clean-all: clean infra-down ## Limpia binarios + Docker volumes
