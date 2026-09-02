// [PORTED_FROM: src/metri/infrastructure/glue.clj]
// infrastructure/glue.rs — Glue schema sync para tablas OLAP Iceberg.
// Zero-Drop Policy: mismo mapeo de tipos Códice→Glue y lógica de sync.

use std::collections::HashSet;

use aws_sdk_glue::Client;
use aws_sdk_glue::types::{Column, StorageDescriptor, TableInput};
use tracing::{error, info, warn};

use crate::codice::{AttrType, CodeRegistry, EngineChannel};

/// Mapeo de tipos Códice → tipos Glue.
/// [PORTED_FROM: (def codice->glue-type {...})]
fn codice_to_glue_type(attr_type: &AttrType) -> &'static str {
    match attr_type {
        AttrType::Decimal              => "double",
        AttrType::Epoch | AttrType::Number => "bigint",
        AttrType::Boolean              => "boolean",
        AttrType::Json                 => "string",
        _                              => "string",
    }
}

/// Cliente de sincronización de schemas Glue.
/// [PORTED_FROM: ig/init-key :infra/glue + sync-all-entity-tables!]
pub struct GlueSyncClient {
    client:   Client,
    database: String,
}

impl GlueSyncClient {
    pub async fn new(database: impl Into<String>) -> Self {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        let db     = database.into();
        info!("[Glue] cliente activo | database: {db}");
        GlueSyncClient { client, database: db }
    }

    /// Sincroniza el schema de TODAS las entidades OLAP con Glue.
    /// [PORTED_FROM: (sync-all-entity-tables! client db)]
    pub async fn sync_all_entity_tables(&self, registry: &CodeRegistry) {
        info!("[Glue Sync] Iniciando sincronización | database: {}", self.database);

        let olap_entities: Vec<String> = registry
            .entity_names()
            .filter(|name| {
                registry
                    .get_engine(name)
                    .map(|e| e == &EngineChannel::Olap)
                    .unwrap_or(false)
            })
            .map(str::to_string)
            .collect();

        info!("[Glue Sync] Modelos OLAP: {}", olap_entities.len());

        for entity_name in &olap_entities {
            if let Err(e) = self.sync_entity_table(entity_name, registry).await {
                error!("[Glue Sync] Error en '{}': {}", entity_name, e);
            }
        }
    }

    /// Sincroniza una tabla Iceberg individual.
    /// [PORTED_FROM: (sync-entity-table! client db model)]
    async fn sync_entity_table(
        &self,
        entity_name: &str,
        registry:    &CodeRegistry,
    ) -> Result<(), String> {
        let table_name  = entity_name.replace('-', "_");
        let new_columns = self.build_entity_columns(entity_name, registry);

        let get_resp = self
            .client
            .get_table()
            .database_name(&self.database)
            .name(&table_name)
            .send()
            .await;

        match get_resp {
            Err(_) => {
                warn!("[Glue Sync] '{table_name}' no encontrada (aún no creada por seeder)");
                Ok(())
            }
            Ok(resp) => {
                let table = resp.table.ok_or("Sin tabla en GetTable")?;

                let current_cols: HashSet<String> = table
                    .storage_descriptor()
                    .map(|sd| sd.columns().iter().map(|c| c.name().to_string()).collect())
                    .unwrap_or_default();

                let new_col_names: HashSet<String> =
                    new_columns.iter().map(|c| c.name().to_string()).collect();

                if current_cols == new_col_names {
                    info!("[Glue Sync] '{}' — esquema al día | {} cols", table_name, new_columns.len());
                    return Ok(());
                }

                info!("[Glue Sync] Actualizando '{table_name}' | {} cols", new_columns.len());

                // Construir StorageDescriptor y TableInput sin to_builder()
                let sd = StorageDescriptor::builder()
                    .set_columns(Some(new_columns))
                    .build();

                let table_input = TableInput::builder()
                    .name(&table_name)
                    .storage_descriptor(sd)
                    .build()
                    .map_err(|e| format!("TableInput error: {e}"))?;

                self.client
                    .update_table()
                    .database_name(&self.database)
                    .table_input(table_input)
                    .send()
                    .await
                    .map_err(|e| format!("UpdateTable falló: {e}"))?;

                info!("[Glue Sync] '{}' — sincronizado", table_name);
                Ok(())
            }
        }
    }

    /// Construye columnas Glue para una entidad.
    /// [PORTED_FROM: (build-entity-columns model)]
    fn build_entity_columns(&self, entity_name: &str, registry: &CodeRegistry) -> Vec<Column> {
        let system_names: HashSet<&str> = ["id", "_tenant", "created_at"].into_iter().collect();

        let mut cols: Vec<Column> = [
            Column::builder().name("id").r#type("string").build(),
            Column::builder().name("_tenant").r#type("string").build(),
            Column::builder().name("created_at").r#type("bigint").build(),
        ]
        .into_iter()
        .flatten()  // flatten Result<Column, _> → Column
        .collect();

        if let Some(attrs) = registry.get_attributes(entity_name) {
            for attr in attrs {
                let col_name = attr.name.replace('-', "_");
                if system_names.contains(col_name.as_str()) {
                    continue;
                }
                let glue_type = codice_to_glue_type(&attr.attr_type);
                if let Ok(col) = Column::builder().name(col_name).r#type(glue_type).build() {
                    cols.push(col);
                }
            }
        }

        cols
    }
}
