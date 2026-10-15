//! Metri Vortex (.mbf) unified HTAP binary storage and execution engine.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::RwLock;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{
    DomainResult, IClock, IMbfObjectStore, MbfConditionalGet, MbfObjectPayload,
};

pub mod format;
pub mod tx;
pub mod zones;

/// Deterministic clock for testing fiscal locks, DST transitions, and time-series windows without sleeping.
pub struct DeterministicClock {
    current_ms: AtomicI64,
}

impl DeterministicClock {
    pub fn new(initial_epoch_ms: i64) -> Self {
        Self {
            current_ms: AtomicI64::new(initial_epoch_ms),
        }
    }

    pub fn advance_ms(&self, delta_ms: i64) -> i64 {
        self.current_ms.fetch_add(delta_ms, Ordering::SeqCst) + delta_ms
    }

    pub fn set_ms(&self, new_epoch_ms: i64) {
        self.current_ms.store(new_epoch_ms, Ordering::SeqCst);
    }
}

impl IClock for DeterministicClock {
    fn now_epoch_ms(&self) -> i64 {
        self.current_ms.load(Ordering::SeqCst)
    }
}

/// In-memory S3-compatible object store with fault injection (`HTTP 412`, `HTTP 304`, byte-range reads, and telemetry counters).
#[derive(Default)]
pub struct InMemoryChaosS3Store {
    objects: RwLock<HashMap<String, MbfObjectPayload>>,
    injected_winners: RwLock<HashMap<String, Vec<u8>>>,
    pub gets_count: AtomicU64,
    pub puts_count: AtomicU64,
    pub range_gets_count: AtomicU64,
    pub not_modified_304_hits: AtomicU64,
    pub conflict_412_count: AtomicU64,
}

impl InMemoryChaosS3Store {
    pub fn new() -> Self {
        Self::default()
    }

    /// Programs the store to simulate a concurrent Lambda instance winning the next `put_if_match` on `key`,
    /// overwriting `key` with `winner_bytes` and returning `MbfCasConflict001` (`HTTP 412`) to the caller.
    pub fn inject_concurrent_winner_on_next_put(&self, key: &str, winner_bytes: Vec<u8>) {
        if let Ok(mut guard) = self.injected_winners.write() {
            guard.insert(key.to_string(), winner_bytes);
        }
    }

    fn compute_etag(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        hex::encode(&digest[0..16])
    }
}

#[async_trait]
impl IMbfObjectStore for InMemoryChaosS3Store {
    async fn get_object(&self, key: &str) -> DomainResult<Option<MbfObjectPayload>> {
        self.gets_count.fetch_add(1, Ordering::Relaxed);
        let guard = self.objects.read().map_err(|_| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Poisoned lock in InMemoryChaosS3Store",
            )
        })?;
        Ok(guard.get(key).cloned())
    }

    async fn get_if_none_match(
        &self,
        key: &str,
        cached_etag: &str,
    ) -> DomainResult<MbfConditionalGet> {
        let guard = self.objects.read().map_err(|_| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Poisoned lock in InMemoryChaosS3Store",
            )
        })?;
        match guard.get(key) {
            None => Ok(MbfConditionalGet::NotFound),
            Some(obj) if obj.etag == cached_etag => {
                self.not_modified_304_hits.fetch_add(1, Ordering::Relaxed);
                Ok(MbfConditionalGet::NotModified)
            }
            Some(obj) => {
                self.gets_count.fetch_add(1, Ordering::Relaxed);
                Ok(MbfConditionalGet::Modified(obj.clone()))
            }
        }
    }

    async fn get_byte_range(&self, key: &str, start: usize, end: usize) -> DomainResult<Vec<u8>> {
        self.range_gets_count.fetch_add(1, Ordering::Relaxed);
        let guard = self.objects.read().map_err(|_| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Poisoned lock in InMemoryChaosS3Store",
            )
        })?;
        let obj = guard.get(key).ok_or_else(|| {
            DomainError::mbf(ErrorCode::MbfCorrupt001, format!("Object not found: {key}"))
        })?;
        if start > end || end > obj.bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!(
                    "Invalid byte range {start}..{end} for len {}",
                    obj.bytes.len()
                ),
            ));
        }
        Ok(obj.bytes[start..end].to_vec())
    }

    async fn put_if_match(
        &self,
        key: &str,
        bytes: Vec<u8>,
        expected_etag: Option<&str>,
    ) -> DomainResult<String> {
        // Check if a one-shot concurrent winner was injected for chaos testing
        let maybe_winner = self
            .injected_winners
            .write()
            .ok()
            .and_then(|mut w| w.remove(key));
        if let Some(winner_bytes) = maybe_winner {
            let winner_etag = Self::compute_etag(&winner_bytes);
            if let Ok(mut guard) = self.objects.write() {
                guard.insert(
                    key.to_string(),
                    MbfObjectPayload {
                        bytes: winner_bytes,
                        etag: winner_etag,
                    },
                );
            }
            self.conflict_412_count.fetch_add(1, Ordering::Relaxed);
            return Err(DomainError::mbf(
                ErrorCode::MbfCasConflict001,
                format!("Simulated HTTP 412 Precondition Failed on {key}"),
            ));
        }

        let mut guard = self.objects.write().map_err(|_| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Poisoned lock in InMemoryChaosS3Store",
            )
        })?;
        match (guard.get(key), expected_etag) {
            (None, None) => {}
            (Some(existing), Some(exp)) if existing.etag == exp => {}
            _ => {
                self.conflict_412_count.fetch_add(1, Ordering::Relaxed);
                return Err(DomainError::mbf(
                    ErrorCode::MbfCasConflict001,
                    format!("ETag mismatch on conditional put for {key}"),
                ));
            }
        }

        let new_etag = Self::compute_etag(&bytes);
        guard.insert(
            key.to_string(),
            MbfObjectPayload {
                bytes,
                etag: new_etag.clone(),
            },
        );
        self.puts_count.fetch_add(1, Ordering::Relaxed);
        Ok(new_etag)
    }
}
