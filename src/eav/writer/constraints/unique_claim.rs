//! Garantía de unicidad mediante un ítem de reclamación (`#CLAIM`) dentro de la misma transacción ACID.
//!
//! Genera un registro independiente en la misma transacción cuya clave primaria (`PK`)
//! se deriva criptográficamente (SHA-256) de la tupla de atributos únicos declarados:
//!
//! ```text
//! PK  = T#<tenant|GLOBAL>#U#<entidad>#<sha256(tupla ordenada)>
//! SK  = b"#CLAIM"
//! eid = <ulid de la entidad propietaria>
//! ```
//!
//! - `CREATE`: `Put(claim)` con `attribute_not_exists(PK)`.
//! - `UPDATE`: `Put(claim)` con `attribute_not_exists(PK) OR eid = :self` (idempotente para la misma entidad).
//! - `DELETE`: el borrado lógico mediante retracción conserva la reclamación histórica.

use super::{ConstraintContext, ConstraintPlanner};
use crate::codice::registry::{ConstraintKind, ConstraintScope, EntityModel};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::DatomValue;
use crate::eav::writer::transact::TransactOp;
use crate::mbf::storage::{AttributeValue, Blob, Put, TransactWriteItem};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Planificador que materializa restricciones `ConstraintKind::Unique` como ítems `#CLAIM` condicionales en la transacción.
pub struct UniqueClaimPlanner;

impl Default for UniqueClaimPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqueClaimPlanner {
    /// Crea una nueva instancia del planificador de unicidad `UniqueClaimPlanner`.
    pub fn new() -> Self {
        UniqueClaimPlanner
    }
}

/// Serializa un valor de forma estable para la huella de la clave.
///
/// Estable en el sentido fuerte: el mismo valor lógico debe producir siempre
/// los mismos bytes, hoy y tras un despliegue. Por eso el tipo forma parte de
/// la cadena — sin él, la cadena "1" y el número 1 colisionarían y dos claves
/// distintas se estorbarían.
fn huella_valor(v: &DatomValue) -> String {
    match v {
        DatomValue::Null => "n:".to_string(),
        DatomValue::Bool(b) => format!("b:{b}"),
        DatomValue::Long(i) => format!("i:{i}"),
        DatomValue::BigInt(i) => format!("I:{i}"),
        DatomValue::Instant(i) => format!("t:{i}"),
        DatomValue::Ref(r) => format!("r:{r}"),
        // Los flotantes no entran en una clave: dos valores que se imprimen
        // igual pueden no ser el mismo bit, y al revés. Una clave natural
        // sobre un flotante es un error de modelado, no un caso a soportar.
        DatomValue::Double(_) => "?:double".to_string(),
        DatomValue::Geo { .. } => "?:geo".to_string(),
        DatomValue::Str(s) => format!("s:{s}"),
        DatomValue::Uuid(s) => format!("u:{s}"),
        DatomValue::Array(a) => format!("a:{}", a.join("\u{1f}")),
        DatomValue::Bytes(b) => format!("y:{}", hex::encode(b)),
    }
}

/// Huella de una tupla de atributos, en el orden declarado.
pub fn huella_clave(
    entity: &str,
    attributes: &[String],
    attrs: &HashMap<String, DatomValue>,
) -> Option<String> {
    let mut hasher = Sha256::new();
    hasher.update(entity.as_bytes());

    for name in attributes {
        // Si falta un atributo de la clave, no hay clave que reclamar. Ocurre
        // en un UPDATE parcial que no toca esos campos: reclamar con lo que
        // haya produciría una clave distinta de la ya escrita, y se
        // duplicaría en lugar de protegerse.
        let value = attrs.get(name)?;
        if matches!(value, DatomValue::Null) {
            return None;
        }
        hasher.update(b"\x1e");
        hasher.update(name.as_bytes());
        hasher.update(b"\x1d");
        hasher.update(huella_valor(value).as_bytes());
    }

    Some(hex::encode(hasher.finalize()))
}

impl ConstraintPlanner for UniqueClaimPlanner {
    fn name(&self) -> &'static str {
        "unique_claim"
    }

    fn applies_to(&self, model: &EntityModel) -> bool {
        model
            .constraints
            .iter()
            .any(|c| c.kind == ConstraintKind::Unique)
            || model.attributes.iter().any(|a| a.unique.is_some())
    }

    fn plan(&self, ctx: &ConstraintContext) -> Result<Vec<TransactWriteItem>, DomainError> {
        if ctx.op == TransactOp::Delete {
            return Ok(Vec::new());
        }

        let mut items = Vec::new();

        for constraint in ctx
            .model
            .constraints
            .iter()
            .filter(|c| c.kind == ConstraintKind::Unique)
        {
            if let Some(claim) =
                Self::build_claim_item(ctx, &constraint.attributes, constraint.scope)?
            {
                items.push(claim);
            }
        }

        for attr in &ctx.model.attributes {
            let Some(unique_kind) = attr.unique.as_deref() else {
                continue;
            };
            let single_attr = [attr.name.clone()];
            let already_declared = ctx.model.constraints.iter().any(|c| {
                c.kind == ConstraintKind::Unique && c.attributes.as_slice() == single_attr
            });
            if already_declared {
                continue;
            }
            let scope = match unique_kind {
                "identity" | "global" => ConstraintScope::Global,
                _ => ConstraintScope::Tenant,
            };
            if let Some(claim) = Self::build_claim_item(ctx, &single_attr, scope)? {
                items.push(claim);
            }
        }

        Ok(items)
    }
}

impl UniqueClaimPlanner {
    fn build_claim_item(
        ctx: &ConstraintContext,
        attributes: &[String],
        scope: ConstraintScope,
    ) -> Result<Option<TransactWriteItem>, DomainError> {
        let Some(huella) = huella_clave(&ctx.model.entity, attributes, ctx.attrs) else {
            return Ok(None);
        };
        if attributes
            .iter()
            .any(|a| matches!(ctx.attrs.get(a), Some(DatomValue::Str(s)) if s.trim().is_empty()))
        {
            return Ok(None);
        }

        let ambito = match scope {
            ConstraintScope::Tenant => ctx.tenant_id,
            ConstraintScope::Global => "GLOBAL",
        };
        let pk = format!("T#{ambito}#U#{}#{huella}", ctx.model.entity);

        let mut item = HashMap::new();
        item.insert("PK".to_string(), AttributeValue::S(pk));
        item.insert(
            "SK".to_string(),
            AttributeValue::B(Blob::new(b"#CLAIM".to_vec())),
        );
        item.insert(
            "eid".to_string(),
            AttributeValue::S(ctx.entity_id.to_string()),
        );
        item.insert("attrs".to_string(), AttributeValue::S(attributes.join(",")));

        let (expr, valores) = match ctx.op {
            TransactOp::Create => ("attribute_not_exists(PK)".to_string(), None),
            _ => (
                "attribute_not_exists(PK) OR eid = :self".to_string(),
                Some((
                    ":self".to_string(),
                    AttributeValue::S(ctx.entity_id.to_string()),
                )),
            ),
        };

        let mut put = Put::builder()
            .table_name(ctx.table)
            .set_item(Some(item))
            .condition_expression(expr);

        if let Some((k, v)) = valores {
            put = put.expression_attribute_values(k, v);
        }

        Ok(Some(
            TransactWriteItem::builder()
                .put(put.build().map_err(|e| {
                    DomainError::eav(
                        ErrorCode::Eav001,
                        format!("no se pudo construir el item de reclamación: {e}"),
                    )
                })?)
                .build(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codice::registry::{Constraint, ConstraintKind, ConstraintScope, EngineChannel};

    fn modelo(constraints: Vec<Constraint>) -> EntityModel {
        EntityModel {
            entity: "tenant_plugin".to_string(),
            label: None,
            icon: None,
            primary_key: None,
            fts_fields: vec![],
            engine: EngineChannel::Oltp,
            attributes: vec![],
            event_rules: vec![],
            is_sequence_scope_provider: false,
            write_path_locked: false,
            is_system: true,
            disable_eda: false,
            shadow_sagas_mapping: None,
            constraints,
            ..Default::default()
        }
    }

    fn unica(attrs: &[&str], scope: ConstraintScope) -> Constraint {
        Constraint {
            kind: ConstraintKind::Unique,
            scope,
            attributes: attrs.iter().map(|s| s.to_string()).collect(),
            when: None,
        }
    }

    fn attrs(pares: &[(&str, &str)]) -> HashMap<String, DatomValue> {
        pares
            .iter()
            .map(|(k, v)| (k.to_string(), DatomValue::Str(v.to_string())))
            .collect()
    }

    // El estado por defecto del sistema: ningún modelo declara restricciones,
    // así que el planificador no debe planificar nada. Es lo que hace que F0
    // sea desplegable por sí sola.
    #[test]
    fn sin_restricciones_declaradas_no_planifica_nada() {
        let planner = UniqueClaimPlanner::new();
        let m = modelo(vec![]);

        assert!(
            !planner.applies_to(&m),
            "un modelo sin constraints no debe entrar al planificador"
        );
    }

    #[test]
    fn la_huella_es_estable_entre_llamadas() {
        let a = attrs(&[("tenant_id", "t1"), ("plugin_id", "cmms")]);
        let attrs_clave = vec!["tenant_id".to_string(), "plugin_id".to_string()];

        let h1 = huella_clave("tenant_plugin", &attrs_clave, &a).unwrap();
        let h2 = huella_clave("tenant_plugin", &attrs_clave, &a).unwrap();

        assert_eq!(h1, h2, "la misma clave debe producir siempre el mismo item");
    }

    #[test]
    fn claves_distintas_producen_huellas_distintas() {
        let attrs_clave = vec!["tenant_id".to_string(), "plugin_id".to_string()];

        let cmms = huella_clave(
            "tenant_plugin",
            &attrs_clave,
            &attrs(&[("tenant_id", "t1"), ("plugin_id", "cmms")]),
        )
        .unwrap();
        let iot = huella_clave(
            "tenant_plugin",
            &attrs_clave,
            &attrs(&[("tenant_id", "t1"), ("plugin_id", "iot")]),
        )
        .unwrap();
        let otro_tenant = huella_clave(
            "tenant_plugin",
            &attrs_clave,
            &attrs(&[("tenant_id", "t2"), ("plugin_id", "cmms")]),
        )
        .unwrap();

        assert_ne!(
            cmms, iot,
            "dos módulos del mismo tenant no pueden compartir reclamación"
        );
        assert_ne!(
            cmms, otro_tenant,
            "el mismo módulo en otro tenant es otra clave"
        );
    }

    // El orden de los atributos forma parte de la clave física. Si no se
    // respetara, reordenar la declaración en el JSON dejaría huérfanos todos
    // los items ya escritos, sin que nada lo dijera.
    #[test]
    fn el_orden_declarado_forma_parte_de_la_clave() {
        let a = attrs(&[("tenant_id", "t1"), ("plugin_id", "cmms")]);

        let directo = huella_clave(
            "tenant_plugin",
            &["tenant_id".to_string(), "plugin_id".to_string()],
            &a,
        )
        .unwrap();
        let inverso = huella_clave(
            "tenant_plugin",
            &["plugin_id".to_string(), "tenant_id".to_string()],
            &a,
        )
        .unwrap();

        assert_ne!(directo, inverso);
    }

    // Dos entidades distintas nunca comparten espacio de claves: el nombre de
    // la entidad entra en la huella.
    #[test]
    fn entidades_distintas_no_colisionan() {
        let a = attrs(&[("tenant_id", "t1"), ("plugin_id", "cmms")]);
        let clave = vec!["tenant_id".to_string(), "plugin_id".to_string()];

        assert_ne!(
            huella_clave("tenant_plugin", &clave, &a).unwrap(),
            huella_clave("domain_quota", &clave, &a).unwrap()
        );
    }

    // Un tipo distinto con la misma representación no puede colisionar: la
    // cadena "1" y el número 1 son claves diferentes.
    #[test]
    fn el_tipo_entra_en_la_huella() {
        let clave = vec!["v".to_string()];
        let como_texto: HashMap<String, DatomValue> =
            [("v".to_string(), DatomValue::Str("1".to_string()))].into();
        let como_numero: HashMap<String, DatomValue> =
            [("v".to_string(), DatomValue::Long(1))].into();

        assert_ne!(
            huella_clave("x", &clave, &como_texto).unwrap(),
            huella_clave("x", &clave, &como_numero).unwrap()
        );
    }

    // Un UPDATE parcial que no toca los campos de la clave no debe reclamar:
    // con lo que hubiera en el payload produciría una clave distinta de la ya
    // escrita, y duplicaría en lugar de proteger.
    #[test]
    fn una_clave_incompleta_no_se_reclama() {
        let clave = vec!["tenant_id".to_string(), "plugin_id".to_string()];

        assert!(huella_clave("tenant_plugin", &clave, &attrs(&[("tenant_id", "t1")])).is_none());
    }

    #[test]
    fn un_nulo_en_la_clave_no_se_reclama() {
        let clave = vec!["a".to_string()];
        let a: HashMap<String, DatomValue> = [("a".to_string(), DatomValue::Null)].into();

        assert!(huella_clave("x", &clave, &a).is_none());
    }

    #[test]
    fn create_planifica_un_item_por_restriccion() {
        let planner = UniqueClaimPlanner::new();
        let m = modelo(vec![unica(
            &["tenant_id", "plugin_id"],
            ConstraintScope::Tenant,
        )]);
        let a = attrs(&[("tenant_id", "t1"), ("plugin_id", "cmms")]);

        let ctx = ConstraintContext {
            tenant_id: "t1",
            entity_id: "01ULID",
            model: &m,
            attrs: &a,
            op: TransactOp::Create,
            table: "metri-dynamo",
        };

        assert!(planner.applies_to(&m));
        assert_eq!(planner.plan(&ctx).unwrap().len(), 1);
    }

    #[test]
    fn delete_no_planifica_reclamaciones() {
        let planner = UniqueClaimPlanner::new();
        let m = modelo(vec![unica(
            &["tenant_id", "plugin_id"],
            ConstraintScope::Tenant,
        )]);
        let a = attrs(&[("tenant_id", "t1"), ("plugin_id", "cmms")]);

        let ctx = ConstraintContext {
            tenant_id: "t1",
            entity_id: "01ULID",
            model: &m,
            attrs: &a,
            op: TransactOp::Delete,
            table: "metri-dynamo",
        };

        assert!(planner.plan(&ctx).unwrap().is_empty());
    }

    #[test]
    fn el_ambito_global_no_depende_del_tenant() {
        let planner = UniqueClaimPlanner::new();
        let m = modelo(vec![unica(&["email"], ConstraintScope::Global)]);
        let a = attrs(&[("email", "x@y.z")]);

        // Se compara la CLAVE, no la representación del item: los atributos
        // viven en un HashMap y su orden de impresión no es estable.
        let pk = |tenant: &str| {
            let ctx = ConstraintContext {
                tenant_id: tenant,
                entity_id: "01ULID",
                model: &m,
                attrs: &a,
                op: TransactOp::Create,
                table: "metri-dynamo",
            };
            let items = planner.plan(&ctx).unwrap();
            let put = items[0].put.as_ref().unwrap();
            match put.item.get("PK").unwrap() {
                AttributeValue::S(v) => v.clone(),
                other => panic!("PK inesperada: {other:?}"),
            }
        };

        let clave = pk("t1");
        assert_eq!(
            clave,
            pk("t2"),
            "una clave global debe ser la misma en cualquier tenant"
        );
        assert!(
            clave.starts_with("T#GLOBAL#"),
            "el ámbito global no lleva tenant: {clave}"
        );
    }

    #[test]
    fn varias_restricciones_producen_varios_items() {
        let planner = UniqueClaimPlanner::new();
        let m = modelo(vec![
            unica(&["tenant_id", "plugin_id"], ConstraintScope::Tenant),
            unica(&["codigo"], ConstraintScope::Tenant),
        ]);
        let a = attrs(&[
            ("tenant_id", "t1"),
            ("plugin_id", "cmms"),
            ("codigo", "A-1"),
        ]);

        let ctx = ConstraintContext {
            tenant_id: "t1",
            entity_id: "01ULID",
            model: &m,
            attrs: &a,
            op: TransactOp::Create,
            table: "metri-dynamo",
        };

        assert_eq!(planner.plan(&ctx).unwrap().len(), 2);
    }
}
