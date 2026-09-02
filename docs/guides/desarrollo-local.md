# Guía — Desarrollo local

> Verificada contra el Makefile · 2 de septiembre de 2026

## Primera vez

```bash
make infra    # DynamoDB Local + MinIO + ElasticMQ (docker compose)
make dev      # engine con hot-reload (cargo-watch) — gRPC en localhost:9090
make seed     # datos de prueba: tenant demo + work_orders
make smoke    # smoke test gRPC con grpcurl
```

## Tests

```bash
make test               # suite unitaria (cargo test)
make test-integration   # tests #[ignore] contra DynamoDB Local (make infra primero)
```

## Lint y formato

```bash
cargo fmt
cargo clippy -- -D warnings   # el CI tiene línea base registrada; no dejarla crecer
```

## Explorar el API

`tonic-reflection` está habilitado: `grpcurl -plaintext localhost:9090 list`.
