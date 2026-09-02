# ADR-006 — La identidad de las entidades de negocio la mintea el engine

**Estado:** aceptada · 2026-09-02

## Contexto

El CREATE de entidades de negocio (`is_system: false` en el Códice) ignoraba el `id`
propuesto por el cliente y generaba un ULID propio, pero el UPDATE confiaba
ciegamente en el id del payload. Un cliente que creaba con `id: "X"` obtenía una
entidad con id `Y` (silenciosamente) y su UPDATE sobre `"X"` fabricaba una
**entidad fantasma**: el writer no encontraba atributos activos, no generaba
retract, y los asserts construían la entidad como upsert — sin `EAV_002`, sin
histórico, sin rastro. El e2e de auditoría (`test_audit_time_travel_history`)
nació para cazar exactamente esto; nunca pudo ejecutarse hasta que compiló
(fase 0), y entonces el hallazgo se verificó datom a datom contra DynamoDB.

El modo silencioso era el problema: el mismo comportamiento que "funcionaba"
amputaba el audit trail del recurso real.

## Decisión

1. **El engine mintea el id de toda entidad de negocio** y lo devuelve como
   único identificador autoritativo en `TransactionResponse.entity_id`.
2. **El cliente no provee identidad en CREATE.** Un payload de negocio con
   `id`/`entity_id`/`ulid` se rechaza con `JANUS_VAL_001` desde la ruta
   (`OltpChannel::route_single` y `route_bulk`) — fail-closed, nunca silencio.
   El puente legacy del servicio que inyectaba el id del RPC en el payload solo
   aplica a UPDATE/DELETE (localizar la entidad); en CREATE se omite y un id
   propuesto por campo RPC se registra con `warn` — el id real viaja en la
   respuesta.
3. **Las entidades de sistema** (`is_system: true`: tenant, role, user…)
   conservan ids canónicos provistos por la plataforma — el seeding maestro
   (`grpc::bootstrap`) depende de ellos.
4. **UPDATE/DELETE exigen existencia**: la entidad no previa es `EAV_002`, no
   un upsert (invariante gemelo, mismo ADR).

## Consecuencias

- Los clientes correctos no cambian nada: ya usaban el `entity_id` devuelto.
- Los clientes que enviaban `id` en CREATE de negocio pasan de perderlo en
  silencio a recibir `JANUS_VAL_001` — visible, diagnóstico inmediato.
- La e2e de auditoría usa el id devuelto y verifica la cadena completa
  CREATE → UPDATE → timeline con retract + assert y atribución de actor.
- Los tests de invariante del writer (`eav::writer::tests`, lote de
  `make test-integration`) blindan: update sin upsert, colisión de datom
  rechazada (`EAV_TX_004`), histórico preservado con par retract/assert.
- Decisión de identidad en un solo lugar: si algún día una integración
  externa necesita traer claves propias, es una política por modelo en el
  Códice (`id_policy`), no una regla repartida por el código.
