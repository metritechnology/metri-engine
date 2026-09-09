//! Local query engine — lake reading with in-memory cache.
//!
//! infrastructure/local_s3_query_engine/lake_reader.rs — Fase 5 (commit D)
//!
//! Responsabilidad 3 del motor local: la lectura del lake — listar objetos
//! del prefijo de la entidad y descargar JSON-lines con caché en memoria
//! (40 descargas concurrentes). Es la ÚNICA pieza con I/O; todo lo demás
//! del motor es puro (sql_parse, pipeline).

use std::collections::HashMap;

use serde_json::Value;

use super::LocalS3QueryEngine;
use crate::domain::errors::{DomainError, ErrorCode};

impl LocalS3QueryEngine {
    /// Lista todos los objetos bajo `iceberg-data/{entity}/` paginando.
    pub(super) async fn list_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<aws_sdk_s3::types::Object>, DomainError> {
        let mut all_objects = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self
                .s3_client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let list_res = req.send().await.map_err(|e| {
                DomainError::aegis(ErrorCode::Aeg004, format!("S3 list falló: {e}"))
            })?;

            if let Some(objects) = list_res.contents {
                all_objects.extend(objects);
            }

            if let Some(token) = list_res.next_continuation_token {
                continuation_token = Some(token.to_string());
            } else {
                break;
            }
        }
        Ok(all_objects)
    }

    /// Descarga con caché: las llaves ya vistas salen de memoria, el resto en
    /// paralelo (40 concurrentes).
    pub(super) async fn fetch_records(
        &self,
        all_objects: Vec<aws_sdk_s3::types::Object>,
    ) -> Result<Vec<serde_json::Map<String, Value>>, DomainError> {
        let mut raw_records: Vec<serde_json::Map<String, Value>> = Vec::new();

        // 1. Identificar qué llaves ya están en caché y cuáles necesitamos descargar
        let mut keys_to_fetch = Vec::new();
        {
            let cache_map = self.cache.lock().map_err(|_| {
                DomainError::aegis(
                    ErrorCode::Aeg004,
                    "Lock poisoned en el cache de LocalS3QueryEngine",
                )
            })?;
            for obj in &all_objects {
                let key = match obj.key() {
                    Some(k) if !k.ends_with('/') => k.to_string(),
                    _ => continue,
                };
                if let Some(cached_rows) = cache_map.get(&key) {
                    raw_records.extend(cached_rows.clone());
                } else {
                    keys_to_fetch.push(key);
                }
            }
        }

        // 2. Descargar en paralelo solo las llaves que no están en el caché
        let mut new_records = Vec::new();
        if !keys_to_fetch.is_empty() {
            tracing::info!(
                "[LocalS3QueryEngine] {} llaves no encontradas en caché. Descargando de S3...",
                keys_to_fetch.len()
            );

            use futures::StreamExt;
            let s3_client = self.s3_client.clone();
            let bucket = self.bucket.clone();

            let mut stream = futures::stream::iter(keys_to_fetch)
                .map(move |key| {
                    let s3 = s3_client.clone();
                    let b = bucket.clone();
                    async move {
                        let get_res = s3.get_object().bucket(&b).key(&key).send().await;
                        match get_res {
                            Ok(res) => match res.body.collect().await {
                                Ok(data) => Some((key, data.into_bytes())),
                                Err(e) => {
                                    tracing::warn!("[LocalS3QueryEngine] body collect falló para key {}: {:?}", key, e);
                                    None
                                }
                            },
                            Err(e) => {
                                tracing::warn!("[LocalS3QueryEngine] get_object falló para key {}: {:?}", key, e);
                                None
                            }
                        }
                    }
                })
                .buffered(40); // 40 requests concurrently

            let mut temp_cache: HashMap<String, Vec<serde_json::Map<String, Value>>> =
                HashMap::new();
            while let Some(maybe_bytes) = stream.next().await {
                if let Some((key, bytes)) = maybe_bytes {
                    let content = String::from_utf8_lossy(&bytes);
                    let mut file_records = Vec::new();
                    for line in content.lines() {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let val: Value = match serde_json::from_str(trimmed) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let Some(obj_map) = val.as_object() {
                            file_records.push(obj_map.clone());
                        }
                    }
                    temp_cache.insert(key, file_records.clone());
                    new_records.extend(file_records);
                }
            }

            // Guardar las nuevas llaves en la caché global
            if !temp_cache.is_empty() {
                if let Ok(mut cache_map) = self.cache.lock() {
                    for (k, v) in temp_cache {
                        cache_map.insert(k, v);
                    }
                }
            }
        }

        raw_records.extend(new_records);
        Ok(raw_records)
    }
}
