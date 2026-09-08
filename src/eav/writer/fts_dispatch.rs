// eav/writer/fts_dispatch.rs — Despacho del índice FTS (Degraded Consistency).
//
// El índice de trigramas vive FUERA de la transacción ACID: BatchWriteItem en
// chunks de 25, con reintentos en el cliente. Dos decisiones de política viven
// aquí y no en el orquestador:
//   1. El spawn ocurre DESPUÉS del commit ACID — si la transacción falla, ya
//      no quedan trigramas de datoms que nunca se confirmaron contaminando el
//      índice con entidades inexistentes.
//   2. Los errores de FTS se registran y nunca fallan la mutación ya
//      confirmada: el índice es reconstruible, la escritura no.

use std::sync::Arc;

use aws_sdk_dynamodb::types::WriteRequest;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::domain::errors::DomainError;
use crate::infrastructure::dynamodb::DynamoClient;

pub struct FtsDispatch {
    handles: Vec<JoinHandle<Result<(), DomainError>>>,
}

impl FtsDispatch {
    /// Lanza la escritura de trigramas en chunks concurrentes de 25.
    pub fn spawn(
        ddb: Arc<DynamoClient>,
        table: impl Into<String>,
        requests: Vec<WriteRequest>,
    ) -> Self {
        let table = table.into();
        let mut handles = Vec::new();
        for chunk in requests.chunks(25) {
            let chunk_vec = chunk.to_vec();
            let ddb_clone = ddb.clone();
            let table_clone = table.clone();
            handles.push(tokio::spawn(async move {
                ddb_clone.batch_write_item(&table_clone, chunk_vec).await
            }));
        }
        FtsDispatch { handles }
    }

    /// Espera a que terminen todos los chunks — no bloquean el ACID, pero
    /// deben terminar antes de responder.
    pub async fn await_all(self) {
        for handle in self.handles {
            if let Ok(Err(e)) = handle.await {
                warn!("[EAV] Error en escritura Batch FTS asíncrona: {e:?}");
            }
        }
    }
}
