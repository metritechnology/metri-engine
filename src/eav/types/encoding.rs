// [NUEVO — sin equivalente directo en Clojure]
// src/eav/types/encoding.rs
// Builders de Sort Key binario para los 4 índices EAV.
// Diseñado según Metri EAV - OLPT.md §II.3 y §II.4
//
// El SK binario garantiza:
//   1. memcmp-compatible ordering — DynamoDB ordena por bytes nativos
//   2. Zero-deserialization para range queries (begins_with, BETWEEN)
//   3. Todos los tipos numéricos son comparables correctamente

use crate::eav::types::datom::DatomValue;
use crate::eav::types::value_type::ValueType;

// ─── EAVT Sort Key ────────────────────────────────────────────────────────────
// PK = "T#<tenant>#E#<eid>"
// SK = [attr_id: 2B big-endian][tx_id: 8B big-endian][op: 1B]
//
// Total: 11 bytes fijos. memcmp-ordering por attr_id → tx_id → op
// Esto permite: "Dame todos los datoms del atributo X de la entidad Y en orden cronológico"
// con un solo Query + ScanIndexForward=true.

pub fn build_eavt_sk(attr_id: u16, tx_id: u64, op: bool) -> Vec<u8> {
    let mut buf = Vec::with_capacity(11);
    buf.extend_from_slice(&attr_id.to_be_bytes()); // 2 bytes
    buf.extend_from_slice(&tx_id.to_be_bytes());   // 8 bytes
    buf.push(if op { 0x01 } else { 0x00 });        // 1 byte
    buf
}

/// Prefijo de SK para buscar todos los datoms de un atributo específico.
/// Usado en Query: SK begins_with [attr_id: 2B]
pub fn eavt_sk_attr_prefix(attr_id: u16) -> Vec<u8> {
    attr_id.to_be_bytes().to_vec()
}

/// SK para buscar la versión de un atributo as-of un tx_id.
/// Usado en Query: SK <= [attr_id][tx_id][0xFF], ScanIndexForward=false, Limit=1
pub fn eavt_sk_as_of(attr_id: u16, as_of_tx: u64) -> Vec<u8> {
    build_eavt_sk(attr_id, as_of_tx, true)
}

// ─── AEVT Sort Key ───────────────────────────────────────────────────────────
// PK = "T#<tenant>#A#<attr_name>"
// SK = [entity_id_bytes: variable (UTF-8)][tx_id: 8B big-endian]
//
// Permite: "Dame todos los entity_ids que tienen el atributo X (AEVT scan por tipo)"

pub fn build_aevt_sk(entity_id: &str, tx_id: u64) -> Vec<u8> {
    let eid_bytes = entity_id.as_bytes();
    let mut buf = Vec::with_capacity(eid_bytes.len() + 9);
    buf.push(eid_bytes.len() as u8);       // 1 byte length prefix
    buf.extend_from_slice(eid_bytes);      // entity_id UTF-8
    buf.extend_from_slice(&tx_id.to_be_bytes()); // 8 bytes tx_id
    buf
}

// ─── AVET Sort Key ────────────────────────────────────────────────────────────
// PK = "T#<tenant>#AV#<attr_name>"
// SK = [type_tag: 1B][value_bytes: variable][entity_id: variable]
//
// Permite range queries: SK >= [type_tag][value_lo], SK <= [type_tag][value_hi]
// para filtros como WHERE cost > 100 O WHERE status = 'OPEN'

/// Construye el SK para el índice AVET.
/// Zero-Drop: replica exactamente la lógica del builder de sort keys del blueprint.
pub fn build_avet_sk(value: &DatomValue, entity_id: &str) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(32);
    let tag = value.value_type().tag();
    buf.push(tag);

    match value {
        DatomValue::Null => {
            // Solo el tag — null es comparable con null
        }

        DatomValue::Bool(b) => {
            buf.push(if *b { 0x01 } else { 0x00 });
        }

        DatomValue::Long(n) | DatomValue::Instant(n) => {
            // Big-endian signed: XOR con 0x8000_0000_0000_0000 para
            // hacer que los negativos sean menores que los positivos en memcmp.
            // [BLUEPRINT: §II.4 — IEEE 754 bit-flip equivalent for i64]
            let flipped = (*n as u64) ^ 0x8000_0000_0000_0000u64;
            buf.extend_from_slice(&flipped.to_be_bytes());
        }

        DatomValue::Double(d) => {
            // IEEE 754 bit-flip: para f64, si el bit de signo está seteado (negativo),
            // invertimos todos los bits. Si no (positivo), solo invertimos el bit de signo.
            // Esto hace que el orden binario coincida con el orden numérico.
            // [BLUEPRINT: §II.4 — IEEE 754 bit-flip para AVET ordering]
            let bits = d.to_bits();
            let normalized = if *d < 0.0 {
                bits ^ u64::MAX           // negativo: invertir todos
            } else {
                bits ^ 0x8000_0000_0000_0000u64  // positivo: invertir solo signo
            };
            buf.extend_from_slice(&normalized.to_be_bytes());
        }

        DatomValue::Str(s) | DatomValue::Uuid(s) => {
            // Truncar a 31 bytes para no exceder el límite de 1024B del SK.
            // [BLUEPRINT: §XII.3 — Truncamiento Predictivo]
            let bytes = s.as_bytes();
            let len = bytes.len().min(31);
            buf.extend_from_slice(&bytes[..len]);
        }

        DatomValue::Ref(eid) => {
            buf.extend_from_slice(&eid.to_be_bytes());
        }

        DatomValue::BigInt(n) => {
            // Truncamos a 16 bytes (128 bits) en big-endian con flip de signo
            let flipped = (*n as u128) ^ 0x8000_0000_0000_0000_0000_0000_0000_0000u128;
            buf.extend_from_slice(&flipped.to_be_bytes());
        }

        DatomValue::Geo { lat, lon } => {
            // Dos f64 con bit-flip — lat primero, lon segundo
            for d in [*lat, *lon] {
                let bits = d.to_bits();
                let normalized = if d < 0.0 { bits ^ u64::MAX } else { bits ^ 0x8000_0000_0000_0000u64 };
                buf.extend_from_slice(&normalized.to_be_bytes());
            }
        }

        DatomValue::Array(_) | DatomValue::Bytes(_) => {
            // No indexables por valor en AVET — solo tag byte
            buf[0] = 0xFF;
        }
    }

    // Append entity_id como desempate (garantiza unicidad en AVET)
    let eid_bytes = entity_id.as_bytes();
    let eid_len = eid_bytes.len().min(26); // dejar espacio para no superar 1024B total
    buf.extend_from_slice(&eid_bytes[..eid_len]);

    buf
}

/// Prefijo del SK para buscar un valor exacto en AVET.
/// Usado en: Query SK begins_with [type_tag][value_bytes]
pub fn avet_sk_prefix(value: &DatomValue) -> Vec<u8> {
    // Mismo encoding que build_avet_sk pero sin el entity_id al final
    build_avet_sk(value, "")
        .into_iter()
        .take_while(|&b| b != 0) // trim trailing zeros del entity_id vacío
        .collect()
}

// ─── VAET Sort Key ────────────────────────────────────────────────────────────
// PK = "T#<tenant>#V#<ref_entity_id>"
// SK = [attr_id: 2B][source_entity_id: variable]
//
// Permite: "Dame todos los entities que referencian a este entity_id (grafo inverso)"

pub fn build_vaet_sk(attr_id: u16, source_entity_id: &str) -> Vec<u8> {
    let eid_bytes = source_entity_id.as_bytes();
    let mut buf = Vec::with_capacity(2 + eid_bytes.len());
    buf.extend_from_slice(&attr_id.to_be_bytes());
    buf.extend_from_slice(eid_bytes);
    buf
}

// ─── Tests de encoding ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eavt_sk_ordering_is_monotonic_by_tx() {
        let sk1 = build_eavt_sk(1, 100, true);
        let sk2 = build_eavt_sk(1, 200, true);
        assert!(sk1 < sk2, "tx=100 debe ser menor que tx=200 en SK binario");
    }

    #[test]
    fn avet_sk_numeric_ordering_handles_negatives() {
        let neg = build_avet_sk(&DatomValue::Double(-5.0), "entity-1");
        let pos = build_avet_sk(&DatomValue::Double(5.0), "entity-1");
        assert!(neg < pos, "-5.0 debe ser menor que 5.0 en AVET SK binario");
    }

    #[test]
    fn avet_sk_long_ordering() {
        let n1 = build_avet_sk(&DatomValue::Long(-100), "e");
        let n2 = build_avet_sk(&DatomValue::Long(0), "e");
        let n3 = build_avet_sk(&DatomValue::Long(100), "e");
        assert!(n1 < n2 && n2 < n3, "orden numérico debe ser preservado en AVET SK");
    }
}
