//! RPC bodies: configuración de la numeración de registros (secuencias).
//!
//! Superficie de escritura ÚNICA de la política de numeración del tenant
//! (prefix/padding/scope_resolution) y del reajuste del contador global —
//! la tabla dedicada metri-sequence-registry que alimenta al generador de
//! códigos (`codice::sequence`). No pasa por Cedar/IOP (como QuotaService):
//! el handler resuelve el principal del JWT, fija el tenant desde él y
//! exige grants `sequence_registry:VIEW` / `sequence_registry:UPDATE` —
//! los wildcards de dominio (`*`) de los roles administradores ya los
//! cubren. La fila global es la POLÍTICA del tenant: manda sobre la config
//! estática del Códice al mintear (`SeqAttrConfig::apply_policy`).

use std::collections::HashMap;

use tonic::{Request, Response, Status};
use tracing::info;

use crate::codice::sequence::{self, ScopeResolution, SeqAttrConfig};
use crate::grpc::pb::{
    ListSequenceConfigsRequest, ListSequenceConfigsResponse, SequenceCounter, SequenceDefaults,
    Status as PbStatus, UpsertSequenceConfigRequest, UpsertSequenceConfigResponse,
};
use crate::grpc::service::MetriGrpcService;
use aws_sdk_dynamodb::types::AttributeValue;

const GRANT_VIEW: &str = "sequence_registry:VIEW";
const GRANT_UPDATE: &str = "sequence_registry:UPDATE";

/// Rango válido del padding: 2 dígitos mínimos de negocio, 8 porque el
/// `pattern` del Códice (`^[A-Z0-9][A-Z0-9-]*\d{2,8}$`) rechazaría números
/// más largos en el alta.
const PADDING_MIN: usize = 1;
const PADDING_MAX: usize = 8;

/// Normaliza el prefijo de la política: MAYÚSCULAS, `[A-Z0-9-]`, ≤16 y
/// siempre terminado en `-` (el formato del código es prefix + número).
/// Vacío → cae al default del Códice.
fn normalize_prefix(input: &str, fallback: &str) -> Result<String, String> {
    let raw = if input.trim().is_empty() {
        fallback
    } else {
        input.trim()
    };
    let upper = raw.to_ascii_uppercase();
    if upper.is_empty() {
        return Err("El prefijo no puede quedar vacío".to_string());
    }
    if upper.len() > 16 {
        return Err("El prefijo no puede exceder 16 caracteres".to_string());
    }
    let mut chars = upper.chars();
    let first = chars.next().unwrap_or_default();
    if !first.is_ascii_alphanumeric() {
        return Err("El prefijo debe empezar con una letra o número".to_string());
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err("El prefijo solo admite letras, números y guiones".to_string());
    }
    let mut out = upper;
    if !out.ends_with('-') {
        out.push('-');
    }
    Ok(out)
}

/// Padding efectivo: 0 (ausente) → default del Códice; negativo → error;
/// si no, en rango.
fn effective_padding(input: i32, fallback: usize) -> Result<usize, String> {
    let padding = match input {
        0 => fallback,
        n if n < 0 => return Err("El padding no puede ser negativo".to_string()),
        n => n as usize,
    };
    if (PADDING_MIN..=PADDING_MAX).contains(&padding) {
        Ok(padding)
    } else {
        Err(format!(
            "El padding debe estar entre {PADDING_MIN} y {PADDING_MAX} dígitos"
        ))
    }
}

/// Modo de numeración efectivo: vacío → default del Códice.
fn effective_resolution(
    input: &str,
    fallback: &ScopeResolution,
) -> Result<ScopeResolution, String> {
    match input.trim() {
        "" => Ok(fallback.clone()),
        "exact" => Ok(ScopeResolution::Exact),
        "nearest_registered" => Ok(ScopeResolution::NearestRegistered),
        other => Err(format!(
            "Modo de numeración desconocido: '{other}' (exact | nearest_registered)"
        )),
    }
}

/// Resuelve la config secuencial del Códice para (entity_type, field_name):
/// la entidad debe existir y el atributo declarar `auto_generate sequential`.
fn resolve_sequential(entity_type: &str, field_name: &str) -> Result<SeqAttrConfig, Status> {
    if entity_type.is_empty() || field_name.is_empty() {
        return Err(Status::invalid_argument(
            "entity_type y field_name son obligatorios",
        ));
    }
    let model = crate::codice::global()
        .get_model(entity_type)
        .ok_or_else(|| {
            Status::not_found(format!(
                "La entidad '{entity_type}' no está declarada en el Códice"
            ))
        })?;
    sequence::parse_seq_defaults(model, field_name).ok_or_else(|| {
        Status::invalid_argument(format!(
            "El atributo '{field_name}' de '{entity_type}' no declara auto_generate sequential"
        ))
    })
}

/// Arma el pb de un contador a partir de una fila (política + valor).
fn counter_pb(
    scope_tag: Option<String>,
    policy: &sequence::SequencePolicy,
    current_value: Option<i64>,
) -> SequenceCounter {
    SequenceCounter {
        scope_tag: scope_tag.unwrap_or_default(),
        prefix: policy.prefix.clone().unwrap_or_default(),
        padding_length: policy.padding.map(|p| p as i32).unwrap_or(0),
        current_value: current_value.unwrap_or(0),
        scope_resolution: policy
            .scope_resolution
            .as_ref()
            .map(|r| r.as_str().to_string())
            .unwrap_or_default(),
        exists: true,
    }
}

fn ok_status() -> Option<PbStatus> {
    Some(PbStatus {
        success: true,
        error_code: String::new(),
        error_message: String::new(),
        error_context: None,
    })
}

impl MetriGrpcService {
    /// Lista la política/contadores de numeración de un campo secuencial:
    /// defaults del Códice, fila global (editable) y contadores por
    /// ubicación (solo lectura — los gestiona la herencia del motor).
    pub(crate) async fn list_sequence_configs_impl(
        &self,
        request: Request<ListSequenceConfigsRequest>,
    ) -> Result<Response<ListSequenceConfigsResponse>, Status> {
        let principal = crate::cedar::get_principal_data(
            &request,
            self.valkey_store.as_ref(),
            self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
            self.dev_auth_bypass,
        )
        .await
        .map_err(|err| Status::unauthenticated(format!("Authentication failed: {}", err.detail)))?;

        let grants = crate::cedar::evaluator::principal_grant_keys(&principal);
        if !grants.contains(GRANT_VIEW) && !grants.contains(GRANT_UPDATE) {
            return Err(Status::permission_denied(format!(
                "Se requiere el permiso {GRANT_VIEW} para consultar la numeración"
            )));
        }

        let req = request.into_inner();
        let defaults = resolve_sequential(&req.entity_type, &req.field_name)?;
        let tenant_id = principal.tenant_id;

        let ddb = self.eav_writer.client();
        let table = sequence::sequence_registry_table();

        // Fila global: la política editable del tenant.
        let global_code = sequence::global_sequence_code(&tenant_id, &req.field_name);
        let global_counter = match ddb.get_item(&table, &global_code, None).await? {
            Some(item) => {
                let (policy, current) = sequence::parse_counter_item(&item);
                counter_pb(None, &policy, current)
            }
            None => SequenceCounter {
                scope_tag: String::new(),
                prefix: String::new(),
                padding_length: 0,
                current_value: 0,
                scope_resolution: String::new(),
                exists: false,
            },
        };

        // Contadores por ubicación: filas scoped del tenant para este campo.
        let mut names = HashMap::new();
        names.insert("#pk".to_string(), "PK".to_string());
        let mut values = HashMap::new();
        values.insert(":t".to_string(), AttributeValue::S(tenant_id.clone()));
        values.insert(
            ":pfx".to_string(),
            AttributeValue::S(format!("{tenant_id}:{}:", req.field_name)),
        );
        let items = ddb
            .scan_with_filter(
                &table,
                "tenant_id = :t AND begins_with(#pk, :pfx)",
                names,
                values,
            )
            .await?;

        let scoped_counters = items
            .iter()
            .filter_map(|item| {
                let pk = item.get("PK").and_then(|v| {
                    if let AttributeValue::S(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })?;
                let scope_tag = sequence::scope_tag_from_pk(&pk, &tenant_id, &req.field_name)?;
                let (policy, current) = sequence::parse_counter_item(item);
                Some(counter_pb(Some(scope_tag), &policy, current))
            })
            .collect();

        Ok(Response::new(ListSequenceConfigsResponse {
            status: ok_status(),
            defaults: Some(SequenceDefaults {
                prefix: defaults.prefix,
                padding_length: defaults.padding as i32,
                scope_resolution: defaults.scope_resolution.as_str().to_string(),
            }),
            global_counter: Some(global_counter),
            scoped_counters,
        }))
    }

    /// Escribe la política global del tenant. El contador solo avanza:
    /// `set_next_value` menor o igual al valor vivo se rechaza en DynamoDB
    /// (ConditionExpression) — retroceder fabricaría duplicados contra el
    /// check `unique` del modelo.
    pub(crate) async fn upsert_sequence_config_impl(
        &self,
        request: Request<UpsertSequenceConfigRequest>,
    ) -> Result<Response<UpsertSequenceConfigResponse>, Status> {
        let principal = crate::cedar::get_principal_data(
            &request,
            self.valkey_store.as_ref(),
            self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
            self.dev_auth_bypass,
        )
        .await
        .map_err(|err| Status::unauthenticated(format!("Authentication failed: {}", err.detail)))?;

        let grants = crate::cedar::evaluator::principal_grant_keys(&principal);
        if !grants.contains(GRANT_UPDATE) {
            return Err(Status::permission_denied(format!(
                "Se requiere el permiso {GRANT_UPDATE} para editar la numeración"
            )));
        }

        let req = request.into_inner();
        let defaults = resolve_sequential(&req.entity_type, &req.field_name)?;

        let prefix =
            normalize_prefix(&req.prefix, &defaults.prefix).map_err(Status::invalid_argument)?;
        let padding = effective_padding(req.padding_length, defaults.padding)
            .map_err(Status::invalid_argument)?;
        let resolution = effective_resolution(&req.scope_resolution, &defaults.scope_resolution)
            .map_err(Status::invalid_argument)?;

        let tenant_id = principal.tenant_id;
        let ddb = self.eav_writer.client();
        let table = sequence::sequence_registry_table();
        let global_code = sequence::global_sequence_code(&tenant_id, &req.field_name);

        // El próximo número deseado (`n`) fija current_value = n - 1, solo
        // si deja el contador ESTRICTAMENTE adelante. Igual al vivo → es un
        // no-op (solo viaja la política); menor → error explícito.
        let set_current_value = match req.set_next_value {
            None => None,
            Some(next) if next < 1 => {
                return Err(Status::invalid_argument(
                    "set_next_value debe ser un número positivo",
                ));
            }
            Some(next) => {
                let current = match ddb.get_item(&table, &global_code, None).await? {
                    Some(item) => sequence::parse_counter_item(&item).1,
                    None => None,
                };
                let candidate = next - 1;
                match current {
                    Some(c) if candidate <= c => {
                        if candidate == c {
                            None
                        } else {
                            return Err(Status::failed_precondition(format!(
                                "El contador ya va por {c} — el próximo número sería {}; no se puede retroceder",
                                c + 1
                            )));
                        }
                    }
                    _ => Some(candidate),
                }
            }
        };

        let applied = ddb
            .sequence_config_upsert(
                &table,
                &global_code,
                &tenant_id,
                &prefix,
                padding,
                resolution.as_str(),
                set_current_value,
            )
            .await?;
        if !applied {
            return Err(Status::failed_precondition(
                "El contador ya está más adelante que el número solicitado — no se puede retroceder",
            ));
        }

        info!(
            "[SequenceConfig] tenant={} {} policy: prefix={prefix} padding={padding} modo={} next={:?}",
            tenant_id,
            req.field_name,
            resolution.as_str(),
            req.set_next_value,
        );

        // Releer para responder con la fila viva.
        let (policy, current) = match ddb.get_item(&table, &global_code, None).await? {
            Some(item) => sequence::parse_counter_item(&item),
            None => (sequence::SequencePolicy::default(), None),
        };

        Ok(Response::new(UpsertSequenceConfigResponse {
            status: ok_status(),
            global_counter: Some(counter_pb(None, &policy, current)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normaliza_prefijo_a_mayusculas_y_guion_final() {
        assert_eq!(normalize_prefix("ot", "WO-").unwrap(), "OT-");
        assert_eq!(normalize_prefix("WO-", "WO-").unwrap(), "WO-");
        assert_eq!(normalize_prefix(" maint ", "WO-").unwrap(), "MAINT-");
        // Vacío cae al default del Códice.
        assert_eq!(normalize_prefix("", "WO-").unwrap(), "WO-");
        assert_eq!(normalize_prefix("  ", "WO-").unwrap(), "WO-");
    }

    #[test]
    fn rechaza_prefijos_invalidos() {
        assert!(
            normalize_prefix("-WO", "WO-").is_err(),
            "no puede empezar en guion"
        );
        assert!(normalize_prefix("OT_OT", "WO-").is_err(), "solo [A-Z0-9-]");
        assert!(normalize_prefix("UN PREFIJO MUY LARGO DEMASIADO", "WO-").is_err());
    }

    #[test]
    fn padding_efectivo_con_default_y_rango() {
        assert_eq!(
            effective_padding(0, 4).unwrap(),
            4,
            "0 = default del Códice"
        );
        assert_eq!(effective_padding(6, 4).unwrap(), 6);
        assert!(effective_padding(-3, 4).is_err());
        assert!(
            effective_padding(9, 4).is_err(),
            "el pattern admite 8 dígitos máx"
        );
    }

    #[test]
    fn modo_efectivo_con_default() {
        assert_eq!(
            effective_resolution("", &ScopeResolution::NearestRegistered).unwrap(),
            ScopeResolution::NearestRegistered
        );
        assert_eq!(
            effective_resolution("exact", &ScopeResolution::NearestRegistered).unwrap(),
            ScopeResolution::Exact
        );
        assert!(effective_resolution("global_continuo", &ScopeResolution::Exact).is_err());
    }
}
