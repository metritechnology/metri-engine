// [PORTED_FROM: src/metri/iop/pipeline.clj]
// iop/pipeline.rs — Motor Railway del IOP.
// En Clojure: (reduce (fn [[tag ctx] step] (if (= :ok tag) (step ctx) [:error ctx])) ...)
// En Rust:    pipeline::run — pure function, cero I/O, cero estado.
//
// Zero-Drop Policy: cortocircuita en el primer Err, propaga sin ejecutar pasos restantes.

use crate::domain::errors::DomainError;

/// Un paso del pipeline: recibe el contexto y retorna Ok(nuevo_ctx) o Err.
/// [PORTED_FROM: step-fn del vector steps en IOP]
pub type Step<Ctx> = Box<dyn Fn(Ctx) -> Result<Ctx, DomainError> + Send + Sync>;

/// Ejecuta pasos secuencialmente. Cortocircuita en el primer Err.
/// Retorna Ok(ctx_final) | Err(DomainError).
///
/// Equivalente EXACTO de la reducción Railway en Clojure:
///   (reduce (fn [[tag ctx] step] (if (= :ok tag) (step ctx) [:error ctx])) [:ok ctx] steps)
///
/// [PORTED_FROM: (run [steps ctx])]
pub fn run<Ctx>(steps: &[Step<Ctx>], ctx: Ctx) -> Result<Ctx, DomainError> {
    steps.iter().fold(Ok(ctx), |acc, step| {
        match acc {
            Ok(current_ctx) => step(current_ctx),
            Err(e)          => Err(e), // cortocircuito — propagar sin ejecutar
        }
    })
}

/// Versión async del pipeline para steps que requieren I/O.
/// [PORTED_FROM: (run steps ctx) — extensión async para Rust]
pub async fn run_async<Ctx: Send>(
    steps: &[Box<dyn Fn(Ctx) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Ctx, DomainError>> + Send>> + Send + Sync>],
    ctx: Ctx,
) -> Result<Ctx, DomainError> {
    let mut current = Ok(ctx);
    for step in steps {
        match current {
            Ok(c)  => { current = step(c).await; }
            Err(e) => return Err(e), // cortocircuito
        }
    }
    current
}

#[cfg(test)]
#[path = "tests/pipeline_tests.rs"]
mod tests;

