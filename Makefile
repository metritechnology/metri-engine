.PHONY: help run build build-release test test-integration check doc ci \
        hooks smoke deploy clean fmt lint result-check result-strict \
        clean-code-check clean-code-strict clean-code-file

# ══════════════════════════════════════════════════════════════════════════════
#  Metri Engine — Makefile (Rust Native · Unified .mbf HTAP Engine)
# ══════════════════════════════════════════════════════════════════════════════

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Servidor gRPC Rust local (autónomo en memoria / .mbf) ────────────────────

run: ## Levanta el servidor gRPC localmente en localhost:9090 (modo stub sin AWS)
	@echo "▶ Levantando Metri Engine en localhost:9090 (modo stub .mbf)..."
	ENVIRONMENT=development \
	S3_MODE=stub \
	SQS_MODE=stub \
	EVENTBRIDGE_MODE=stub \
	cargo run

# ── Compilación y Verificación Rust local ────────────────────────────────────

build: ## Compila el binario en modo debug
	cargo build

build-release: ## Compila el binario en modo release (idéntico a Lambda)
	cargo build --release

fmt: ## Formatea el código con rustfmt
	cargo fmt

lint: ## Ejecuta clippy
	cargo clippy -- -D warnings

doc: ## Genera rustdoc verificando que no existan enlaces intra-doc rotos
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --lib

result-check: ## Auditoría del patrón Result en modo ratchet (no-regresión)
	python3 scripts/dev/check_result_pattern.py

result-strict: ## Auditoría del patrón Result estricta (lo que corre CI)
	python3 scripts/dev/check_result_pattern.py --strict

clean-code-check: ## Auditoría por archivo de SOLID, DRY y Clean Code en modo ratchet
	python3 scripts/dev/check_clean_code.py

clean-code-strict: ## Auditoría estricta de SOLID, DRY y Clean Code (meta 0 violaciones)
	python3 scripts/dev/check_clean_code.py --strict

clean-code-file: ## Scorecard 0-100 de SOLID, DRY y Clean Code para un archivo (FILE=src/...)
	python3 scripts/dev/check_clean_code.py --file $(FILE)

check: ## cargo check rápido
	cargo check

ci: ## Espejo local del CI (rust.yml): fmt, audits, cargo doc, gate clippy y tests
	cargo fmt --check
	python3 scripts/docs/gen_reference.py --check
	python3 scripts/dev/check_docs.py
	python3 scripts/dev/check_result_pattern.py --strict
	python3 scripts/dev/check_clean_code.py
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --lib
	cargo clippy --all-targets > clippy_local.txt 2>&1 || true
	@bash -c 'if grep -q "^error" clippy_local.txt; then \
		echo "✗ clippy: errores (no negociables):"; grep "^error" clippy_local.txt; exit 1; fi; \
	COUNT=$$(grep -c "^warning" clippy_local.txt || true); \
	echo "Hallazgos clippy: $$COUNT (línea base: 88)"; \
	if [ "$$COUNT" -gt 88 ]; then \
		echo "✗ Clippy creció sobre la línea base (88): $$COUNT"; exit 1; fi'
	cargo test --lib
	cargo test --test architecture_solid_guard
	@echo "✅ Espejo CI en verde — lo que vas a pushar es lo que CI va a aprobar"

hooks: ## Instala el pre-commit hook (fmt + referencia + guard de fuentes)
	git config core.hooksPath scripts/dev/hooks
	@echo "✅ hooks activos (scripts/dev/hooks) — desactivar: git config --unset core.hooksPath"

# ── Tests ────────────────────────────────────────────────────────────────────

test: ## Ejecuta todos los tests (unitarios + integración)
	cargo test

test-integration: ## Ejecuta todas las suites de integración y E2E en tests/
	cargo test --test '*'

smoke: ## Smoke test gRPC contra el engine local (requiere make run)
	grpcurl -plaintext localhost:9090 list
	grpcurl -plaintext localhost:9090 describe metri.MetriService

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
