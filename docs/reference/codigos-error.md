# Códigos de error

> Generado desde `config/errors/error_catalog.toml` (catálogo canónico).
> Regenerar: `python3 scripts/docs/gen_reference.py`

Versión del catálogo: **3.0.0**

## Familia `aeg` (12 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `AEG_001` | aegis | 400 | INVALID_ARGUMENT | no | Aegis received AST IR with unknown engine type — must be oltp or olap *(reservado)* |
| `AEG_002` | aegis | 500 | INTERNAL | sí | Aegis OLTP: EAV query execution failed |
| `AEG_003` | aegis | 404 | NOT_FOUND | no | Aegis OLTP pull: entity_id not found or belongs to a different tenant ⚠ **deprecado** |
| `AEG_004` | aegis | 500 | INTERNAL | sí | Aegis OLAP: IQueryEngine.start_query failed — Athena could not initiate execution ⚠ **deprecado** |
| `AEG_005` | aegis | 500 | INTERNAL | no | Aegis OLAP: IQueryEngine.get_query_results failed — Athena execution error or timeout ⚠ **deprecado** |
| `AEG_006` | aegis | 504 | DEADLINE_EXCEEDED | no | Aegis OLAP: Athena polling timeout — execution_id preserved for reconciliation |
| `AEG_007` | aegis | 500 | INTERNAL | no | Aegis: Athena output parse error — unexpected result shape |
| `AEG_008` | aegis | 400 | INVALID_ARGUMENT | no | Aegis: aggregation function not supported |
| `AEG_COMPILE_001` | aegis | 400 | INVALID_ARGUMENT | no | Aegis OLTP: failed to compile AST IR to EAV QueryPlan — unsupported operator or malformed node |
| `AEG_COMPILE_002` | aegis | 400 | INVALID_ARGUMENT | no | Aegis OLAP: failed to compile AST IR to SQL — unsupported operator or malformed node |
| `AEG_COMPILE_003` | aegis | 400 | INVALID_ARGUMENT | no | Aegis SQL compilation failed — query could not be compiled |
| `AEG_TENANT_MISSING` | aegis | 500 | INTERNAL | no | Aegis received an AST IR without tenant isolation in :where — data exfiltration risk blocked |

## Familia `aud` (2 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `AUD_001` | audit-interceptor | 500 | INTERNAL | no | AuditInterceptor.audit failed to write to OLAPChannel — audit record lost |
| `AUD_002` | audit-interceptor | 500 | INTERNAL | no | audit/derive_action_type returned unknown — fallback to WRITE applied |

## Familia `cdx` (5 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `CDX_001` | codice | 500 | INTERNAL | no | Códice bootstrap failed — cannot load entity models from config/models/. Engine halted. |
| `CDX_002` | codice | 400 | INVALID_ARGUMENT | no | Model validation failed — entity JSON schema does not conform to Códice contract |
| `CDX_003` | codice | 404 | NOT_FOUND | no | Entity type not found in Códice registry — unknown entity_type in request |
| `CDX_004` | codice | 500 | INTERNAL | no | Códice SHA-256 fingerprint collision detected — registry integrity compromised |
| `COD_MAT_001` | codice | 400 | INVALID_ARGUMENT | no | Materialization spec references unknown entity/attribute/hook |

## Familia `eav` (11 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `EAV_001` | eav-writer | 500 | INTERNAL | no | EAV transact build_write_items failed — Put builder error constructing DynamoDB items |
| `EAV_002` | eav-reader | 500 | INTERNAL | sí | EAV query execution failed — DynamoDB GSI query returned error (AVET/AEVT/VAET) *(reservado)* |
| `EAV_003` | eav-cursor | 400 | INVALID_ARGUMENT | no | Cursor stale or invalid — query fingerprint changed between pages or cursor is malformed ⚠ **deprecado** |
| `EAV_004` | eav-registry | 400 | INVALID_ARGUMENT | no | entity_type or attribute not found in AttributeRegistry — schema validation failed |
| `EAV_005` | eav-reader | 404 | NOT_FOUND | no | EAV entity not found for the given entity_id/tenant |
| `EAV_FTS_001` | eav-fts | 500 | INTERNAL | no | EAV FTS index write failed — trigram batch rejected |
| `EAV_KEY_001` | eav-writer | 400 | INVALID_ARGUMENT | no | EAV sort key overflow — key exceeds 1024 bytes |
| `EAV_TX_001` | eav-writer | 500 | INTERNAL | sí | DynamoDB TransactWriteItems returned TransactionCanceledException |
| `EAV_TX_002` | eav-writer | 507 | RESOURCE_EXHAUSTED | no | EAV transaction exceeded 100-item DynamoDB limit after chunking — degraded consistency mode activated *(reservado)* |
| `EAV_TX_003` | eav-writer | 409 | ABORTED | sí | Optimistic locking failed — entity version changed between read and write (ConcurrentModification) |
| `EAV_TX_004` | eav-writer | 409 | ABORTED | sí | Datom write collision — conditional write detected an existing (attr_id, tx_id, op); the append-only history invariant refused the overwrite. Retry re-mints a fresh tx_id |

## Familia `fml` (13 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `FML_001` | aegis::formula::lexer | 400 | INVALID_ARGUMENT | no | Unexpected token in formula expression |
| `FML_002` | aegis::formula::parser | 400 | INVALID_ARGUMENT | no | Unbalanced parentheses in formula |
| `FML_003` | aegis::formula::parser | 400 | INVALID_ARGUMENT | no | Unknown function invoked in formula |
| `FML_004` | aegis::formula::parser | 400 | INVALID_ARGUMENT | no | Function arity mismatch — wrong number of arguments |
| `FML_005` | aegis::formula::evaluator | 200 | OK | no | Division by zero in formula evaluation (row skipped) |
| `FML_006` | aegis::formula::evaluator | 200 | OK | no | Math domain error in formula (e.g., SQRT(-1), LOG(0)) |
| `FML_007` | aegis::formula::security | 403 | PERMISSION_DENIED | no | SQL injection pattern detected in formula (word-boundary match) |
| `FML_008` | aegis::formula::lexer | 400 | INVALID_ARGUMENT | no | Formula exceeds maximum length |
| `FML_009` | aegis::formula::parser | 400 | INVALID_ARGUMENT | no | Parenthesis nesting depth exceeds limit |
| `FML_010` | aegis::formula::lexer | 400 | INVALID_ARGUMENT | no | Formula token count exceeds limit |
| `FML_011` | aegis::formula::parser | 400 | INVALID_ARGUMENT | no | Function nesting depth exceeds limit |
| `FML_012` | aegis::formula::evaluator | 200 | OK | no | Variable not found in row context during evaluation |
| `FML_013` | aegis::formula::lexer | 400 | INVALID_ARGUMENT | no | Empty formula string received |

## Familia `grpc` (6 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `GRPC_500` | grpc-service | 500 | INTERNAL | no | Unhandled internal server error in gRPC handler — see tracing span for details |
| `GRPC_AUTH_001` | grpc-interceptor | 401 | UNAUTHENTICATED | no | HMAC-SHA256 signature verification failed — token invalid, expired, or tampered |
| `GRPC_AUTH_002` | grpc-interceptor | 401 | UNAUTHENTICATED | no | Authorization header missing or malformed — Bearer token not found *(reservado)* |
| `GRPC_AUTH_003` | grpc-interceptor | 401 | UNAUTHENTICATED | no | Token revoked — present in the revocation blacklist |
| `GRPC_AUTH_004` | grpc-interceptor | 403 | PERMISSION_DENIED | no | Tenant mismatch — authenticated principal denied access to the requested tenant |
| `GRPC_TENANT_001` | grpc-interceptor | 400 | INVALID_ARGUMENT | no | tenant_id missing or empty in request metadata — request rejected before processing |

## Familia `infra` (18 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `INFRA_ATHENA_001` | athena | 500 | INTERNAL | sí | Athena StartQueryExecution failed — SDK error or quota exceeded *(reservado)* |
| `INFRA_ATHENA_002` | athena | 500 | INTERNAL | no | Athena query FAILED state — query execution error returned by Athena engine *(reservado)* |
| `INFRA_ATHENA_003` | athena | 500 | INTERNAL | no | Athena GetQueryResults failed — cannot retrieve results after successful execution *(reservado)* |
| `INFRA_ATHENA_004` | athena | 504 | DEADLINE_EXCEEDED | no | Athena query exceeded polling timeout — execution_id preserved for async reconciliation *(reservado)* |
| `INFRA_ATHENA_005` | athena | 403 | PERMISSION_DENIED | no | Athena AccessDeniedException — Lambda IAM role lacks athena:StartQueryExecution or s3:PutObject on output bucket |
| `INFRA_CATALOG_001` | error-catalog | 500 | INTERNAL | no | Error catalog failed to load or validate at bootstrap — engine halted |
| `INFRA_CEDAR_001` | cedar | 503 | UNAVAILABLE | no | Cedar Policy Engine unavailable or policy parsing error — authorization cannot be evaluated *(reservado)* |
| `INFRA_CEDAR_002` | cedar | 503 | UNAVAILABLE | no | Cedar policy store could not be loaded from DynamoDB during bootstrap — ABAC is non-functional |
| `INFRA_CONFIG_001` | config | 500 | INTERNAL | no | Engine configuration failed to resolve or validate at bootstrap — engine halted |
| `INFRA_DDB_001` | dynamodb | 500 | INTERNAL | sí | DynamoDB SDK error — unexpected failure on GetItem/PutItem/Query/TransactWriteItems |
| `INFRA_DDB_002` | dynamodb | 429 | RESOURCE_EXHAUSTED | sí | DynamoDB ProvisionedThroughputExceededException — read/write capacity exhausted on partition *(reservado)* |
| `INFRA_DDB_003` | dynamodb | 404 | NOT_FOUND | no | DynamoDB item not found — entity may not exist or tenant isolation filtered it out *(reservado)* |
| `INFRA_EB_001` | eventbridge | 500 | INTERNAL | no | EventBridge SDK error — event put failed |
| `INFRA_FIREHOSE_001` | firehose | 500 | INTERNAL | sí | Firehose SDK error — delivery stream operation failed |
| `INFRA_GLUE_001` | glue | 500 | INTERNAL | sí | Glue SDK error — Iceberg table sync failed |
| `INFRA_KIN_001` | kinesis | 500 | INTERNAL | no | Kinesis SDK error — record put failed |
| `INFRA_S3_001` | s3 | 500 | INTERNAL | sí | S3 SDK error — unexpected failure on S3 operation |
| `INFRA_SQS_001` | sqs | 500 | INTERNAL | sí | SQS SDK error — unexpected failure on SQS operation |

## Familia `iop` (4 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `IOP_001` | iop | 400 | INVALID_ARGUMENT | no | IOP integrity validation failed — payload rejected before Janus |
| `IOP_002` | iop | 500 | INTERNAL | no | IOP enrichment failed |
| `IOP_003` | iop | 500 | INTERNAL | sí | IOP ACID transaction failed — full write rolled back |
| `IOP_004` | iop | 500 | INTERNAL | no | IOP outbox publish failed — event delivery at risk |

## Familia `janus` (6 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `JANUS_400` | janus-cerebro | 400 | INVALID_ARGUMENT | no | Janus rejected request — tenant_id empty, cedar_ctx invalid, AST missing tenant, enum sentinel=0, or scope=NONE |
| `JANUS_401` | janus-cerebro | 401 | UNAUTHENTICATED | no | Janus rejected request — authentication token invalid or expired |
| `JANUS_403` | janus-cerebro | 403 | PERMISSION_DENIED | no | Janus rejected request — Cedar DENY (cross-tenant or insufficient grant) |
| `JANUS_404` | janus-cerebro | 404 | NOT_FOUND | no | Janus request target not found |
| `JANUS_422` | janus-cerebro | 422 | INVALID_ARGUMENT | no | Janus request semantically invalid — payload or reference failed validation |
| `JANUS_VAL_001` | janus-validator | 400 | INVALID_ARGUMENT | no | Janus Validator rejected request — schema validation failed against AST IR contract |

## Familia `jns` (9 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `JNS_001` | janus | 500 | INTERNAL | no | No channel registered for the given engine type |
| `JNS_CONFLICT_001` | janus | 409 | ABORTED | no | unique:value constraint violated — value already exists in EAV AVET index |
| `JNS_LOCK_001` | janus | 423 | PERMISSION_DENIED | no | write_path_locked: the entity is read-only — mutations are blocked |
| `JNS_OLAP_003` | janus | 403 | PERMISSION_DENIED | no | BulkIngest attempted on entity without engine:olap or write_path_locked:true |
| `JNS_REF_001` | janus | 422 | INVALID_ARGUMENT | no | Referenced entity UUID does not exist in the EAV store *(reservado)* |
| `JNS_REF_002` | janus | 422 | INVALID_ARGUMENT | no | Referenced entity UUID exists but has wrong entity type (type confusion attack blocked) |
| `JNS_SCOPE_001` | janus | 500 | INTERNAL | sí | scope_resolution found no valid sequence registry entry for the given scope_tag |
| `JNS_SEED_001` | janus | 403 | PERMISSION_DENIED | no | is_system_seeded: entity is bootstrapped by the system and cannot be mutated by clients |
| `JNS_TX_001` | janus | 500 | INTERNAL | sí | EAV transact failed — full transaction rolled back (entity + all index projections) |

## Familia `mcp` (1 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `MCP_503` | mcp | 503 | UNAVAILABLE | sí | LLM service unavailable — Anthropic or Ollama timeout or capacity exceeded |

## Familia `quota` (1 códigos)

| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |
|---|---|---|---|---|---|
| `QUOTA_001` | quota | 429 | RESOURCE_EXHAUSTED | sí | Tenant quota exhausted — request rejected by the quota ledger |
