.PHONY: repl build build-uberjar test-all check-cedar clean build-MetriEngineFunction sync-iceberg sync-firehose

# ── SAM Build target ─────────────────────────────────────────────────────────
# Invocado por: sam build (Metadata.BuildMethod: makefile)
# SAM pasa ARTIFACTS_DIR como destino; copiamos el uberjar pre-compilado.
# El uberjar DEBE estar en target/ antes de ejecutar sam build.
build-MetriEngineFunction:
	mkdir -p $(ARTIFACTS_DIR)/lib
	cp target/metri-engine.jar $(ARTIFACTS_DIR)/lib/
	cp -r resources/models $(ARTIFACTS_DIR)/models


repl:
	@echo "Starting Clojure REPL with metri-dev profile..."
	AWS_PROFILE=metri-dev clj -M:dev

build:
	@echo "Enforcing AOT constraints & building GraalVM native artifact..."
	clj -T:build native

build-uberjar:
	@echo "Building generic uberjar (AWS SnapStart approach)..."
	docker run --rm --network host --entrypoint bash -v $$(pwd):/app -w /app -v ~/.m2:/root/.m2 clojure:tools-deps -c "apt-get update && apt-get install -y protobuf-compiler wget && wget -qO /usr/local/bin/protoc-gen-grpc-java https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.62.2/protoc-gen-grpc-java-1.62.2-linux-aarch_64.exe && chmod +x /usr/local/bin/protoc-gen-grpc-java && PROTOC_BIN=protoc PROTOC_INC=/usr/include PROTOC_PLUGIN=/usr/local/bin/protoc-gen-grpc-java clj -T:build uber"

test-all:
	@echo "Running all tests..."
	AWS_PROFILE=metri-dev clj -M:test

check-cedar:
	@echo "Validating AVP policies syntax..."
	@if command -v cedar-policy-cli > /dev/null; then \
		cedar-policy-cli validate -p policies.cedar -s schema.cedar; \
	else \
		echo "Warning: cedar-policy-cli not installed locally, skipping local syntax check."; \
	fi

sync-iceberg:
	@echo "Sincronizando modelos OLAP de Códice con tablas Apache Iceberg en AWS Athena..."
	docker run --rm -v $$(pwd):/app -w /app -v ~/.aws:/root/.aws -v ~/.m2:/root/.m2 -e AWS_PROFILE=metri-dev clojure:tools-deps clj -X metri.codice.iceberg-seeder/sync-tables!

sync-firehose:
	@echo "Sincronizando modelos OLAP de Códice con streams Kinesis Firehose (upsert idempotente)..."
	docker run --rm -v $$(pwd):/app -w /app -v ~/.aws:/root/.aws -v ~/.m2:/root/.m2 -e AWS_PROFILE=metri-dev clojure:tools-deps clj -X metri.codice.firehose-seeder/sync-streams!

deploy:
	@echo "Desplegando Infraestructura Serverless..."
	sam build
	sam deploy --no-confirm-changeset --profile metri-dev
	$(MAKE) sync-iceberg
	$(MAKE) sync-firehose

clean:
	@echo "Cleaning target directory..."
	rm -rf target/
