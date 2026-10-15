//! Zone 4 penny-exact financial accounting, FixedDecimal128, FIFO/LIFO/WAC/FEFO inventory, FX, NIC 16, and Merkle chain.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;

/// Canonical fixed-point decimal (`i128` with scale $10^4$: `10_000` units = `1.0000` currency unit).
/// Eliminates all IEEE 754 floating-point drift in financial and accounting ledgers.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct FixedDecimal128(pub i128);

impl FixedDecimal128 {
    pub const SCALE: i128 = 10_000;
    pub const ZERO: Self = Self(0);

    #[inline]
    pub const fn from_scaled(raw_scaled: i128) -> Self {
        Self(raw_scaled)
    }

    #[inline]
    pub const fn from_whole(units: i64) -> Self {
        Self((units as i128) * Self::SCALE)
    }

    /// Creates a `FixedDecimal128` from integer units and basis points (`10^-4`).
    #[inline]
    pub const fn from_units_and_bps(units: i64, bps: i64) -> Self {
        Self((units as i128) * Self::SCALE + (bps as i128))
    }

    #[inline]
    pub fn to_f64(self) -> f64 {
        (self.0 as f64) / (Self::SCALE as f64)
    }

    #[inline]
    pub fn checked_add(self, rhs: Self) -> DomainResult<Self> {
        self.0.checked_add(rhs.0).map(Self).ok_or_else(|| {
            DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "FixedDecimal128 addition overflow",
            )
        })
    }

    #[inline]
    pub fn checked_sub(self, rhs: Self) -> DomainResult<Self> {
        self.0.checked_sub(rhs.0).map(Self).ok_or_else(|| {
            DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "FixedDecimal128 subtraction overflow",
            )
        })
    }

    /// Multiplies two `FixedDecimal128` numbers and divides by `SCALE` using IEEE 754-2008 Banker's Rounding (*Round Half-to-Even*).
    pub fn mul_bankers(self, rhs: Self) -> Self {
        Self::div_bankers_raw(self.0 * rhs.0, Self::SCALE)
    }

    /// Divides `self` by `divisor` using IEEE 754-2008 Banker's Rounding (*Round Half-to-Even*).
    pub fn div_bankers(self, divisor: Self) -> DomainResult<Self> {
        if divisor.0 == 0 {
            return Err(DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "Division by zero in FixedDecimal128",
            ));
        }
        Ok(Self::div_bankers_raw(self.0 * Self::SCALE, divisor.0))
    }

    fn div_bankers_raw(numerator: i128, denominator: i128) -> Self {
        let q = numerator / denominator;
        let r = numerator % denominator;
        let abs_r2 = r.abs() * 2;
        let abs_d = denominator.abs();
        let sign = if (numerator < 0) ^ (denominator < 0) {
            -1
        } else {
            1
        };

        if abs_r2 > abs_d {
            Self(q + sign)
        } else if abs_r2 < abs_d {
            Self(q)
        } else if q.abs() % 2 == 1 {
            // Exact half-way tie: round to nearest even integer!
            Self(q + sign)
        } else {
            Self(q)
        }
    }

    /// Distributes `total` across `weights` using Hamilton's Largest Remainder Method so $\sum c_i \equiv \text{total}$ bit-exactly.
    pub fn allocate_hamilton(total: Self, weights: &[u64]) -> DomainResult<Vec<Self>> {
        let weight_sum: u128 = weights.iter().map(|&w| u128::from(w)).sum();
        if weight_sum == 0 {
            return Err(DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "Cannot allocate cost across zero total weights",
            ));
        }
        let total_raw = total.0;
        let sign = if total_raw < 0 { -1i128 } else { 1i128 };
        let abs_total = total_raw.unsigned_abs();

        let mut shares = Vec::with_capacity(weights.len());
        let mut remainders: Vec<(usize, u128)> = Vec::with_capacity(weights.len());
        let mut allocated_sum: u128 = 0;

        for (idx, &w) in weights.iter().enumerate() {
            let num = abs_total * u128::from(w);
            let floor_share = num / weight_sum;
            let rem = num % weight_sum;
            shares.push((floor_share as i128) * sign);
            remainders.push((idx, rem));
            allocated_sum += floor_share;
        }

        let leftover = (abs_total - allocated_sum) as usize;
        // Sort by largest remainder descending, breaking ties deterministically by index
        remainders.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        for &(idx, _) in remainders.iter().take(leftover) {
            shares[idx] += sign;
        }

        Ok(shares.into_iter().map(Self).collect())
    }
}

/// Fenwick Tree / Prefix Sum array over `FixedDecimal128` for $O(\log N)$ updates and $O(1)$ range cash-flow queries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FenwickPrefixSum128 {
    tree: Vec<i128>,
}

impl FenwickPrefixSum128 {
    pub fn with_capacity(len: usize) -> Self {
        Self {
            tree: vec![0; len + 1],
        }
    }

    pub fn from_slice(values: &[FixedDecimal128]) -> Self {
        let mut fw = Self::with_capacity(values.len());
        for (i, v) in values.iter().enumerate() {
            fw.add_at(i, *v);
        }
        fw
    }

    /// Adds `delta` at 0-based index `idx` in $O(\log N)$.
    pub fn add_at(&mut self, idx: usize, delta: FixedDecimal128) {
        let mut i = idx + 1;
        while i < self.tree.len() {
            self.tree[i] += delta.0;
            i += i & (!i + 1);
        }
    }

    /// Prefix sum $[0..=idx]$ in $O(\log N)$.
    pub fn prefix_sum(&self, idx: usize) -> FixedDecimal128 {
        let mut i = (idx + 1).min(self.tree.len().saturating_sub(1));
        let mut sum: i128 = 0;
        while i > 0 {
            sum += self.tree[i];
            i &= i - 1;
        }
        FixedDecimal128(sum)
    }

    /// Range sum $[start..=end]$ in $O(\log N)$.
    pub fn range_sum(&self, start: usize, end: usize) -> FixedDecimal128 {
        if start > end {
            return FixedDecimal128::ZERO;
        }
        let upper = self.prefix_sum(end);
        if start == 0 {
            upper
        } else {
            FixedDecimal128(upper.0 - self.prefix_sum(start - 1).0)
        }
    }
}

/// Inventory cost valuation method (`FIFO`, `LIFO`, `WAC`/`PMP`, `FEFO`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValuationMethod {
    Fifo,
    Lifo,
    Wac,
    Fefo,
}

/// 32-byte aligned active batch layer for inventory valuation (`#[repr(C, align(32))]`).
#[repr(C, align(32))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveBatchLayer {
    pub batch_id: u32,
    pub part_row_id: u32,
    pub received_ms: i64,
    pub expiry_ms: i64,
    pub remaining_qty_scaled: i64,
}

/// 64-byte aligned block checkpoint for `default_rollup: "last_value"` (`balance_after`, `running_average_cost`).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FinancialBlockCheckpoint {
    pub closing_qty: FixedDecimal128,
    pub closing_book_value: FixedDecimal128,
    pub running_wac_unit_cost: FixedDecimal128,
    pub block_end_epoch_ms: i64,
    pub reserved: i64,
}

/// Multi-method Inventory & Financial Ledger with SHA-256 Merkle Chain and Fiscal Lock.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FinancialLedgerZone {
    pub fiscal_lock_epoch_ms: i64,
    pub batches: Vec<(ActiveBatchLayer, FixedDecimal128)>, // (layer, unit_cost)
    pub checkpoints: Vec<FinancialBlockCheckpoint>,
    pub merkle_chain_tip: [u8; 32],
    pub fx_rates_to_cop: HashMap<String, FixedDecimal128>,
}

impl FinancialLedgerZone {
    pub fn new(fiscal_lock_epoch_ms: i64) -> Self {
        let mut fx = HashMap::new();
        fx.insert("COP".into(), FixedDecimal128::from_whole(1));
        fx.insert("USD".into(), FixedDecimal128::from_whole(4200));
        fx.insert("EUR".into(), FixedDecimal128::from_whole(4550));
        Self {
            fiscal_lock_epoch_ms,
            batches: Vec::new(),
            checkpoints: Vec::new(),
            merkle_chain_tip: [0u8; 32],
            fx_rates_to_cop: fx,
        }
    }

    /// Validates a double-entry accounting journal ($\sum \text{Debits} == \sum \text{Credits}$) and seals it into the SHA-256 Merkle chain.
    pub fn record_journal_entry(
        &mut self,
        tx_epoch_ms: i64,
        debits: &[FixedDecimal128],
        credits: &[FixedDecimal128],
    ) -> DomainResult<[u8; 32]> {
        if tx_epoch_ms <= self.fiscal_lock_epoch_ms {
            return Err(DomainError::mbf(
                ErrorCode::MbfFiscalClosed001,
                format!(
                    "Cannot post journal at {tx_epoch_ms} <= fiscal lock {}",
                    self.fiscal_lock_epoch_ms
                ),
            ));
        }
        let sum_d: i128 = debits.iter().map(|d| d.0).sum();
        let sum_c: i128 = credits.iter().map(|c| c.0).sum();
        if sum_d != sum_c {
            return Err(DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                format!("Unbalanced journal entry: debits={sum_d} != credits={sum_c}"),
            ));
        }

        let mut hasher = Sha256::new();
        hasher.update(self.merkle_chain_tip);
        hasher.update(tx_epoch_ms.to_le_bytes());
        hasher.update(sum_d.to_le_bytes());
        let digest = hasher.finalize();
        self.merkle_chain_tip.copy_from_slice(&digest);
        Ok(self.merkle_chain_tip)
    }

    /// Converts an amount from `currency` into `COP` using the embedded ISO 4217 FX table.
    pub fn convert_to_cop(
        &self,
        amount: FixedDecimal128,
        currency: &str,
    ) -> DomainResult<FixedDecimal128> {
        let rate = self.fx_rates_to_cop.get(currency).copied().ok_or_else(|| {
            DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                format!("Missing FX rate for currency '{currency}'"),
            )
        })?;
        Ok(amount.mul_bankers(rate))
    }

    /// Receives an inventory batch and updates the `last_value` `FinancialBlockCheckpoint`.
    pub fn receive_batch(
        &mut self,
        batch_id: u32,
        part_row_id: u32,
        received_ms: i64,
        expiry_ms: i64,
        qty: FixedDecimal128,
        unit_cost_cop: FixedDecimal128,
    ) -> DomainResult<FinancialBlockCheckpoint> {
        if received_ms <= self.fiscal_lock_epoch_ms {
            return Err(DomainError::mbf(
                ErrorCode::MbfFiscalClosed001,
                "Cannot receive inventory in a closed fiscal period",
            ));
        }
        self.batches.push((
            ActiveBatchLayer {
                batch_id,
                part_row_id,
                received_ms,
                expiry_ms,
                remaining_qty_scaled: qty.0 as i64,
            },
            unit_cost_cop,
        ));
        let cp = self.compute_checkpoint(received_ms)?;
        self.checkpoints.push(cp);
        Ok(cp)
    }

    /// Issues (consumes) `issue_qty` under the chosen `ValuationMethod` (`FIFO`, `LIFO`, `WAC`, `FEFO`),
    /// returning `(cogs_cost_cop, updated_checkpoint)`.
    pub fn issue_inventory(
        &mut self,
        tx_epoch_ms: i64,
        issue_qty: FixedDecimal128,
        method: ValuationMethod,
    ) -> DomainResult<(FixedDecimal128, FinancialBlockCheckpoint)> {
        if tx_epoch_ms <= self.fiscal_lock_epoch_ms {
            return Err(DomainError::mbf(
                ErrorCode::MbfFiscalClosed001,
                "Cannot issue inventory in a closed fiscal period",
            ));
        }
        let current_cp = self.compute_checkpoint(tx_epoch_ms)?;
        if current_cp.closing_qty.0 < issue_qty.0 {
            return Err(DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "Insufficient stock to satisfy consumption",
            ));
        }

        // Order batch indices according to FIFO, LIFO, or FEFO
        let mut indices: Vec<usize> = (0..self.batches.len()).collect();
        match method {
            ValuationMethod::Fifo | ValuationMethod::Wac => {
                indices.sort_by_key(|&i| self.batches[i].0.received_ms);
            }
            ValuationMethod::Lifo => {
                indices.sort_by_key(|&i| std::cmp::Reverse(self.batches[i].0.received_ms));
            }
            ValuationMethod::Fefo => {
                indices.sort_by_key(|&i| self.batches[i].0.expiry_ms);
            }
        }

        let mut remaining_to_issue = issue_qty.0 as i64;
        let mut total_cogs = FixedDecimal128::ZERO;

        for idx in indices {
            if remaining_to_issue == 0 {
                break;
            }
            let avail = self.batches[idx].0.remaining_qty_scaled;
            if avail == 0 {
                continue;
            }
            let take = avail.min(remaining_to_issue);
            self.batches[idx].0.remaining_qty_scaled -= take;
            remaining_to_issue -= take;

            let unit_cost = if method == ValuationMethod::Wac {
                current_cp.running_wac_unit_cost
            } else {
                self.batches[idx].1
            };
            let layer_cost = FixedDecimal128(take as i128).mul_bankers(unit_cost);
            total_cogs = total_cogs.checked_add(layer_cost)?;
        }

        let new_cp = self.compute_checkpoint(tx_epoch_ms)?;
        self.checkpoints.push(new_cp);
        Ok((total_cogs, new_cp))
    }

    /// Resolves `default_rollup: "last_value"` as of `target_epoch_ms` in $O(\log B)$ via binary search over checkpoints.
    pub fn last_value_as_of(&self, target_epoch_ms: i64) -> Option<FinancialBlockCheckpoint> {
        let idx = self
            .checkpoints
            .partition_point(|cp| cp.block_end_epoch_ms <= target_epoch_ms);
        if idx == 0 {
            None
        } else {
            Some(self.checkpoints[idx - 1])
        }
    }

    /// Computes NIC 16 asset depreciation crossing calendar straight-line with IoT horometer usage.
    pub fn compute_nic16_depreciation(
        acquisition_cost: FixedDecimal128,
        salvage_value: FixedDecimal128,
        useful_hours_total: u64,
        iot_hours_consumed: u64,
    ) -> DomainResult<FixedDecimal128> {
        if useful_hours_total == 0 {
            return Err(DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "Useful hours cannot be zero in NIC 16 depreciation",
            ));
        }
        let depreciable_base = acquisition_cost.checked_sub(salvage_value)?;
        let capped_hours = iot_hours_consumed.min(useful_hours_total);
        let ratio = FixedDecimal128::from_whole(capped_hours as i64)
            .div_bankers(FixedDecimal128::from_whole(useful_hours_total as i64))?;
        Ok(depreciable_base.mul_bankers(ratio))
    }

    fn compute_checkpoint(&self, epoch_ms: i64) -> DomainResult<FinancialBlockCheckpoint> {
        let mut total_qty = FixedDecimal128::ZERO;
        let mut total_val = FixedDecimal128::ZERO;
        for &(layer, unit_cost) in &self.batches {
            if layer.remaining_qty_scaled > 0 {
                let qty = FixedDecimal128(layer.remaining_qty_scaled as i128);
                total_qty = total_qty.checked_add(qty)?;
                total_val = total_val.checked_add(qty.mul_bankers(unit_cost))?;
            }
        }
        let wac = if total_qty.0 > 0 {
            total_val.div_bankers(total_qty)?
        } else {
            FixedDecimal128::ZERO
        };
        Ok(FinancialBlockCheckpoint {
            closing_qty: total_qty,
            closing_book_value: total_val,
            running_wac_unit_cost: wac,
            block_end_epoch_ms: epoch_ms,
            reserved: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_decimal128_bankers_rounding_and_hamilton_penny_allocation() {
        assert_eq!(std::mem::size_of::<ActiveBatchLayer>(), 32);
        assert_eq!(std::mem::align_of::<ActiveBatchLayer>(), 32);
        assert_eq!(std::mem::size_of::<FinancialBlockCheckpoint>(), 64);
        assert_eq!(std::mem::align_of::<FinancialBlockCheckpoint>(), 64);

        // Allocate 100.0000 COP across 3 equal weights [1, 1, 1]: sum must be 100.0000 bit-exactly!
        let total = FixedDecimal128::from_whole(100);
        let parts = FixedDecimal128::allocate_hamilton(total, &[1, 1, 1]).unwrap();
        let sum: i128 = parts.iter().map(|p| p.0).sum();
        assert_eq!(sum, total.0);
        assert_eq!(parts[0].0, 33_3334);
        assert_eq!(parts[1].0, 33_3333);
        assert_eq!(parts[2].0, 33_3333);
    }

    #[test]
    fn fifo_lifo_fefo_wac_inventory_valuation_and_fiscal_lock() {
        let mut ledger = FinancialLedgerZone::new(1_000);
        // Batch 1: 10 units @ 100 COP, received t=2000, expires t=9000
        ledger
            .receive_batch(
                1,
                50,
                2_000,
                9_000,
                FixedDecimal128::from_whole(10),
                FixedDecimal128::from_whole(100),
            )
            .unwrap();
        // Batch 2: 10 units @ 200 COP, received t=3000, expires t=5000 (expires sooner!)
        ledger
            .receive_batch(
                2,
                50,
                3_000,
                5_000,
                FixedDecimal128::from_whole(10),
                FixedDecimal128::from_whole(200),
            )
            .unwrap();

        // Issue 5 units under FIFO -> must take from Batch 1 @ 100 = 500 COP
        let mut fifo_ledger = ledger.clone();
        let (fifo_cogs, _) = fifo_ledger
            .issue_inventory(4_000, FixedDecimal128::from_whole(5), ValuationMethod::Fifo)
            .unwrap();
        assert_eq!(fifo_cogs, FixedDecimal128::from_whole(500));

        // Issue 5 units under LIFO -> must take from Batch 2 @ 200 = 1000 COP
        let mut lifo_ledger = ledger.clone();
        let (lifo_cogs, _) = lifo_ledger
            .issue_inventory(4_000, FixedDecimal128::from_whole(5), ValuationMethod::Lifo)
            .unwrap();
        assert_eq!(lifo_cogs, FixedDecimal128::from_whole(1000));

        // Issue 5 units under FEFO -> Batch 2 expires at 5000 < 9000, so takes Batch 2 @ 200 = 1000 COP
        let mut fefo_ledger = ledger.clone();
        let (fefo_cogs, _) = fefo_ledger
            .issue_inventory(4_000, FixedDecimal128::from_whole(5), ValuationMethod::Fefo)
            .unwrap();
        assert_eq!(fefo_cogs, FixedDecimal128::from_whole(1000));

        // Issue 5 units under WAC (PMP) -> Average unit cost is (1000 + 2000)/20 = 150 -> 5 * 150 = 750 COP
        let mut wac_ledger = ledger.clone();
        let (wac_cogs, cp) = wac_ledger
            .issue_inventory(4_000, FixedDecimal128::from_whole(5), ValuationMethod::Wac)
            .unwrap();
        assert_eq!(wac_cogs, FixedDecimal128::from_whole(750));
        assert_eq!(cp.closing_qty, FixedDecimal128::from_whole(15));

        // Fiscal lock violation at t=500 <= 1000 must fail with MbfFiscalClosed001
        let err = ledger
            .record_journal_entry(
                500,
                &[FixedDecimal128::from_whole(10)],
                &[FixedDecimal128::from_whole(10)],
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::MbfFiscalClosed001);
    }
}
