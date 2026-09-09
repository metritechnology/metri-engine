//! Bridge from proto/FBS time types to the canonical temporal module.
//!
//! aegis/temporal_bridge.rs
//! Puente entre los tipos proto/FBS del gRPC y el módulo temporal canónico.
//!
//! SRP: traducir TimeFrameContext (proto gRPC) → temporal::TimeFrameCtx,
//! y exponer resolve_for_proto / resolve_for_fbs como API unificada.
//!
//! Todos los consumidores de Aegis (OLTP executor, SQL compiler, router)
//! deben pasar por este bridge en lugar de interpretar los enums proto directamente.

use crate::temporal::core::TimeRange;
use crate::temporal::time_frame::{resolve_time_frame, TimeFrameCtx, TimeFrameType};

// ── Adaptador proto → temporal ────────────────────────────────────────────────

/// Convierte `time_frame_context::TimeFilterType` (i32 proto) → `TimeFrameType`.
/// Garantiza que cualquier valor proto desconocido llega como `Unspecified`
/// (retornará None en resolve_time_frame → sin restricción temporal).
fn proto_type_to_temporal(v: i32) -> TimeFrameType {
    TimeFrameType::from_proto(v)
}

/// Convierte un `crate::grpc::gen::metri::TimeFrameContext` proto
/// → `TimeFrameCtx` de temporal, → `TimeRange` resuelto (epoch-s).
///
/// - `start_ts` / `end_ts` del proto son epoch-**milisegundos** → `ms_to_s` interno.
/// - `n_value` de i32 → i64.
/// - `timezone` con fallback a "UTC".
pub fn resolve_proto_time_frame(tf: &crate::grpc::pb::TimeFrameContext) -> Option<TimeRange> {
    let ctx = TimeFrameCtx {
        tf_type: proto_type_to_temporal(tf.r#type),
        n_value: tf.n_value as i64,
        start_ts_ms: tf.start_ts,
        end_ts_ms: tf.end_ts,
        timezone: if tf.timezone.is_empty() {
            "UTC".to_string()
        } else {
            tf.timezone.clone()
        },
    };
    resolve_time_frame(&ctx)
}

/// Convierte un `crate::janus::fbs::TimeFrameContextT` FlatBuffer
/// → `TimeRange` resuelto (epoch-s).
///
/// El enum FBS usa valores numéricos idénticos al proto → `from_proto` reutilizable.
pub fn resolve_fbs_time_frame(tf: &crate::janus::fbs::TimeFrameContextT) -> Option<TimeRange> {
    let ctx = TimeFrameCtx {
        tf_type: proto_type_to_temporal(tf.type_.0),
        n_value: tf.n_value as i64,
        start_ts_ms: tf.start_ts,
        end_ts_ms: tf.end_ts,
        timezone: tf.timezone.as_deref().unwrap_or("UTC").to_string(),
    };
    resolve_time_frame(&ctx)
}

/// Retorna `TimeRange { start_ts: None, end_ts: None }` cuando no hay TimeFrame
/// (equivalente a ALL_TIME — sin restricción temporal).
pub fn no_time_range() -> TimeRange {
    TimeRange {
        start_ts: None,
        end_ts: None,
    }
}
