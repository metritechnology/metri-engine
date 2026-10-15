//! Zone 1 PAX columnar storage with MVCC time-travel visibility vectors and dictionary encoding.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::mbf::format::MbfCellValue;

/// MVCC row visibility metadata for snapshot isolation and time-travel queries (`as_of_tx`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvccRowVersion {
    pub row_id: u32,
    pub tx_assert_epoch: u64,
    pub tx_retract_epoch: u64, // u64::MAX means currently active (not retracted)
}

impl MvccRowVersion {
    pub const ACTIVE_EPOCH: u64 = u64::MAX;

    /// Returns true if this row version was alive at `snapshot_epoch`.
    pub fn is_visible_at(&self, snapshot_epoch: u64) -> bool {
        self.tx_assert_epoch <= snapshot_epoch && snapshot_epoch < self.tx_retract_epoch
    }
}

/// Zone 1 PAX + MVCC block storing entities with O(1) primary key lookup and time-travel history.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PaxMvccZone {
    pub entity_type: String,
    pub primary_key_index: HashMap<String, Vec<u32>>,
    pub mvcc_versions: Vec<MvccRowVersion>,
    pub columns: HashMap<String, Vec<MbfCellValue>>,
}

impl PaxMvccZone {
    pub fn new(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            primary_key_index: HashMap::new(),
            mvcc_versions: Vec::new(),
            columns: HashMap::new(),
        }
    }

    /// Upserts an entity version at `epoch_id`, automatically retracting any previous active version.
    pub fn upsert_entity(
        &mut self,
        entity_id: &str,
        epoch_id: u64,
        attributes: HashMap<String, MbfCellValue>,
    ) -> u32 {
        if let Some(existing_rows) = self.primary_key_index.get(entity_id) {
            for &r_id in existing_rows {
                if let Some(ver) = self.mvcc_versions.get_mut(r_id as usize) {
                    if ver.tx_retract_epoch == MvccRowVersion::ACTIVE_EPOCH {
                        ver.tx_retract_epoch = epoch_id;
                    }
                }
            }
        }

        let new_row_id = self.mvcc_versions.len() as u32;
        self.mvcc_versions.push(MvccRowVersion {
            row_id: new_row_id,
            tx_assert_epoch: epoch_id,
            tx_retract_epoch: MvccRowVersion::ACTIVE_EPOCH,
        });
        self.primary_key_index
            .entry(entity_id.to_string())
            .or_default()
            .push(new_row_id);

        let total_rows = self.mvcc_versions.len();
        for col in self.columns.values_mut() {
            while col.len() < total_rows {
                col.push(MbfCellValue::Null);
            }
        }
        for (attr_name, val) in attributes {
            let col = self
                .columns
                .entry(attr_name)
                .or_insert_with(|| vec![MbfCellValue::Null; total_rows]);
            while col.len() < total_rows {
                col.push(MbfCellValue::Null);
            }
            if let Some(slot) = col.get_mut(new_row_id as usize) {
                *slot = val;
            }
        }
        new_row_id
    }

    /// Pulls an entity's attributes as of `snapshot_epoch` (Time-Travel Pull).
    pub fn pull_as_of(
        &self,
        entity_id: &str,
        snapshot_epoch: u64,
    ) -> Option<HashMap<String, MbfCellValue>> {
        let rows = self.primary_key_index.get(entity_id)?;
        let active_row_id = rows.iter().rev().copied().find(|&r_id| {
            self.mvcc_versions
                .get(r_id as usize)
                .is_some_and(|v| v.is_visible_at(snapshot_epoch))
        })?;

        let mut map = HashMap::new();
        for (col_name, values) in &self.columns {
            if let Some(val) = values.get(active_row_id as usize) {
                if *val != MbfCellValue::Null {
                    map.insert(col_name.clone(), val.clone());
                }
            }
        }
        Some(map)
    }

    /// Returns all visible `row_id`s at `snapshot_epoch`.
    pub fn visible_rows_at(&self, snapshot_epoch: u64) -> Vec<u32> {
        self.mvcc_versions
            .iter()
            .filter(|v| v.is_visible_at(snapshot_epoch))
            .map(|v| v.row_id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvcc_time_travel_pull_and_retraction_work_accurately() {
        let mut zone = PaxMvccZone::new("work_order");
        let mut v1 = HashMap::new();
        v1.insert("status".into(), MbfCellValue::Text("OPEN".into()));
        v1.insert("total_cost".into(), MbfCellValue::Decimal128(100_0000));
        zone.upsert_entity("WO-100", 1, v1);

        let mut v2 = HashMap::new();
        v2.insert("status".into(), MbfCellValue::Text("COMPLETED".into()));
        v2.insert("total_cost".into(), MbfCellValue::Decimal128(250_0000));
        zone.upsert_entity("WO-100", 5, v2);

        // As of epoch 3, status was OPEN and cost was 100.0000
        let snap_e3 = zone.pull_as_of("WO-100", 3).unwrap();
        assert_eq!(
            snap_e3.get("status"),
            Some(&MbfCellValue::Text("OPEN".into()))
        );
        assert_eq!(
            snap_e3.get("total_cost"),
            Some(&MbfCellValue::Decimal128(100_0000))
        );

        // As of epoch 5, status is COMPLETED and cost is 250.0000
        let snap_e5 = zone.pull_as_of("WO-100", 5).unwrap();
        assert_eq!(
            snap_e5.get("status"),
            Some(&MbfCellValue::Text("COMPLETED".into()))
        );
        assert_eq!(
            snap_e5.get("total_cost"),
            Some(&MbfCellValue::Decimal128(250_0000))
        );
        assert_eq!(zone.visible_rows_at(5).len(), 1);
    }
}
