.PHONY: help infra infra-down engine dev watch build build-release test check \
        seed smoke deploy clean fmt lint

# ══════════════════════════════════════════════════════════════════════════════
#  Metri Engine — Makefile (Rust Native)
# ══════════════════════════════════════════════════════════════════════════════

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Infraestructura local (sin engine) ───────────────────────────────────────

infra: ## Levanta DynamoDB Local
	@echo "▶ Levantando DynamoDB Local..."
	docker compose up -d dynamodb-local
	@ok=0; for i in $$(seq 1 30); do curl -s -o /dev/null http://localhost:8000 && ok=1 && break; sleep 1; done; \
	if [ "$$ok" != "1" ]; then echo "✗ DynamoDB Local no respondió en :8000"; exit 1; fi
	@echo "✅ DynamoDB Local listo en :8000 — tablas: make seed"

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
	@# Los tests `#[ignore]` que viven dentro de src/ (quota::*, eda::moira)
	@# no los recoge `--test '*'`, que solo mira los targets de tests/.
	DYNAMODB_ENDPOINT=http://localhost:8000 \
	EAV_TABLE=metri-eav-local \
	SCHEMAS_TABLE=metri-schemas-local \
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	AWS_DEFAULT_REGION=us-east-1 \
	cargo test --lib quota:: -- --ignored

# ── Seed ─────────────────────────────────────────────────────────────────────

seed: ## Recrea las tablas locales (metri-eav/schemas/quota-local)
	python3 scripts/dev/reset_local.py --recreate

smoke: ## Smoke test gRPC contra el engine local (requiere make engine)
	grpcurl -plaintext localhost:9090 list
	grpcurl -plaintext localhost:9090 describe metri.MetriService

# ── Docker image ──────────────────────────────────────────────────────────────

docker-build: ## Compila la imagen Docker del engine (release)
	docker compose build engine

# ── Deploy AWS ───────────────────────────────────────────────────────────────

deploy: build-release ## Build + deploy SAM a AWS
	@echo "▶ Desplegando a AWS..."
	rm -rf target/debug
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
	cp -r config/prompts $(ARTIFACTS_DIR)/prompts

# ── Limpieza ──────────────────────────────────────────────────────────────────

clean: ## Limpia binarios compilados
	cargo clean
	@echo "✅ target/ limpiado."

clean-all: clean infra-down ## Limpia binarios + Docker volumes
