//! Zona 4 del formato MBF: contabilidad exacta en punto fijo (`FixedDecimal128`), valoración de lotes (`FIFO`, `LIFO`, `WAC`, `FEFO`), tabla cambiaria multimoneda, amortización lineal y cadena Merkle SHA-256.

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::pad_to_64;

const ZONE4_MAGIC: [u8; 8] = *b"MBFZ4FIN";

/// Decimal canónico en punto fijo de 128 bits (`i128`, `#[repr(transparent)]`).
///
/// Elimina toda deriva de punto flotante IEEE 754 en libros contables y financieros.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Pod,
    Zeroable,
    Serialize,
    Deserialize,
)]
pub struct FixedDecimal128(pub i128);

impl FixedDecimal128 {
    /// Factor de escala base ($10^4$, equivalente a puntos básicos / diezmilésimas).
    pub const SCALE: i128 = 10_000;
    /// Factor de escala en microunidades ($10^6$).
    pub const MICRO_SCALE: i128 = 1_000_000;
    /// Constante aditiva neutra (`0.0000`).
    pub const ZERO: Self = Self(0);

    /// Construye un `FixedDecimal128` directamente a partir del valor entero ya escalado por `SCALE`.
    #[inline]
    pub const fn from_scaled(raw_scaled: i128) -> Self {
        Self(raw_scaled)
    }

    /// Construye un `FixedDecimal128` a partir de unidades enteras sin fracción.
    #[inline]
    pub const fn from_whole(units: i64) -> Self {
        Self((units as i128) * Self::SCALE)
    }

    /// Crea un `FixedDecimal128` a partir de microunidades ($10^{-6}$) aplicando redondeo bancario (*Banker's Rounding*).
    #[inline]
    pub fn from_micro_units(micro_units: i128) -> Self {
        Self::div_bankers_raw(micro_units, 100)
    }

    /// Convierte este valor a microunidades canónicas ($10^{-6}$).
    #[inline]
    pub const fn to_micro_units(self) -> i128 {
        self.0 * 100
    }

    /// Crea un `FixedDecimal128` combinando unidades enteras y puntos básicos (`10^-4`).
    #[inline]
    pub const fn from_units_and_bps(units: i64, bps: i64) -> Self {
        Self((units as i128) * Self::SCALE + (bps as i128))
    }

    /// Convierte el decimal en punto fijo a `f64` para visualización o telemetría analítica.
    #[inline]
    pub fn to_f64(self) -> f64 {
        (self.0 as f64) / (Self::SCALE as f64)
    }

    /// Suma verificada de dos valores `FixedDecimal128`, devolviendo error en caso de desbordamiento.
    #[inline]
    pub fn checked_add(self, rhs: Self) -> DomainResult<Self> {
        self.0.checked_add(rhs.0).map(Self).ok_or_else(|| {
            DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "FixedDecimal128 addition overflow",
            )
        })
    }

    /// Resta verificada de dos valores `FixedDecimal128`, devolviendo error en caso de desbordamiento.
    #[inline]
    pub fn checked_sub(self, rhs: Self) -> DomainResult<Self> {
        self.0.checked_sub(rhs.0).map(Self).ok_or_else(|| {
            DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "FixedDecimal128 subtraction overflow",
            )
        })
    }

    /// Multiplica dos números `FixedDecimal128` y divide por `SCALE` usando redondeo bancario IEEE 754-2008 (*Round Half-to-Even*).
    pub fn mul_bankers(self, rhs: Self) -> Self {
        Self::div_bankers_raw(self.0 * rhs.0, Self::SCALE)
    }

    /// Divide `self` entre `divisor` usando redondeo bancario IEEE 754-2008 (*Round Half-to-Even*).
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

    /// Distribuye `total` entre los pesos `weights` mediante el método de mayores restos de Hamilton de modo que $\sum c_i \equiv \text{total}$ de forma exacta bit a bit.
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

/// Árbol de Fenwick (arreglo de sumas prefijas indexado en binario) sobre `FixedDecimal128` para actualizaciones en $O(\log N)$ y consultas de flujo de caja por rango.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenwickPrefixSum128 {
    /// Arreglo interno 1-indexado que almacena las sumas parciales del árbol de Fenwick.
    pub tree: Vec<i128>,
}

impl FenwickPrefixSum128 {
    /// Crea un árbol de Fenwick vacío con capacidad para `len` elementos.
    pub fn with_capacity(len: usize) -> Self {
        Self {
            tree: vec![0; len + 1],
        }
    }

    /// Construye un árbol de Fenwick inicializado a partir de una rebanada de valores `FixedDecimal128`.
    pub fn from_slice(values: &[FixedDecimal128]) -> Self {
        let mut fw = Self::with_capacity(values.len());
        for (i, v) in values.iter().enumerate() {
            fw.add_at(i, *v);
        }
        fw
    }

    /// Suma `delta` en el índice base cero `idx` en tiempo $O(\log N)$.
    pub fn add_at(&mut self, idx: usize, delta: FixedDecimal128) {
        let mut i = idx + 1;
        while i < self.tree.len() {
            self.tree[i] += delta.0;
            i += i & (!i + 1);
        }
    }

    /// Calcula la suma prefija en el rango inclusivo `` `[0..=idx]` `` en tiempo $O(\log N)$.
    pub fn prefix_sum(&self, idx: usize) -> FixedDecimal128 {
        let mut i = (idx + 1).min(self.tree.len().saturating_sub(1));
        let mut sum: i128 = 0;
        while i > 0 {
            sum += self.tree[i];
            i &= i - 1;
        }
        FixedDecimal128(sum)
    }

    /// Calcula la suma acumulada en el rango inclusivo `` `[start..=end]` `` en tiempo $O(\log N)$.
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

    /// Devuelve la rebanada subyacente de enteros `i128` del árbol de Fenwick.
    pub fn as_raw_slice(&self) -> &[i128] {
        &self.tree
    }
}

/// Método de valoración de costos de inventario y lotes (`FIFO`, `LIFO`, `WAC`/`PMP`, `FEFO`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValuationMethod {
    /// Primero en entrar, primero en salir (*First-In, First-Out*).
    Fifo,
    /// Último en entrar, primero en salir (*Last-In, First-Out*).
    Lifo,
    /// Costo promedio ponderado (*Weighted Average Cost* / PMP).
    Wac,
    /// Primero en vencer, primero en salir (*First-Expired, First-Out*).
    Fefo,
}

/// Capa activa de lote alineada a 32 bytes para valoración de recursos e inventario (`#[repr(C, align(32))]`).
#[repr(C, align(32))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct ActiveBatchLayer {
    /// Identificador numérico único del lote recibido.
    pub batch_id: u32,
    /// Identificador de fila del recurso o ítem de inventario asociado.
    pub resource_row_id: u32,
    /// Marca de tiempo de recepción del lote en milisegundos epoch UTC.
    pub received_ms: i64,
    /// Marca de tiempo de caducidad del lote en milisegundos epoch UTC (para valoración `FEFO`).
    pub expiry_ms: i64,
    /// Cantidad remanente disponible en el lote, escalada por `FixedDecimal128::SCALE`.
    pub remaining_qty_scaled: i64,
}

/// Punto de control financiero de bloque alineado a 64 bytes para agregaciones `last_value` (`balance_after`, `running_average_cost`).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Pod, Zeroable, Serialize, Deserialize)]
pub struct FinancialBlockCheckpoint {
    /// Cantidad total de cierre al finalizar el bloque o movimiento.
    pub closing_qty: FixedDecimal128,
    /// Valor contable total en libros al cierre del movimiento.
    pub closing_book_value: FixedDecimal128,
    /// Costo unitario promedio ponderado (`WAC`) vigente tras el movimiento.
    pub running_wac_unit_cost: FixedDecimal128,
    /// Marca de tiempo en milisegundos epoch UTC del cierre del bloque o movimiento.
    pub block_end_epoch_ms: i64,
    /// Campo reservado para mantener la alineación exacta a 64 bytes.
    pub reserved: i64,
}

/// Cabecera criptográfica de auditoría de partida doble alineada a 64 bytes (`#[repr(C, align(64))]`).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Pod, Zeroable, Serialize, Deserialize)]
pub struct LedgerAuditHeader {
    /// Hash SHA-256 del bloque o asiento contable inmediatamente anterior en la cadena.
    pub prev_block_sha256: [u8; 32],
    /// Raíz Merkle / resumen SHA-256 resultante tras sellar el asiento actual.
    pub block_merkle_root: [u8; 32],
}

/// Libro mayor financiero en punto fijo de partida doble y valoración de lotes con cadena Merkle SHA-256 y bloqueo fiscal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinancialLedgerZone {
    /// Marca de tiempo en milisegundos epoch UTC hasta la cual el período fiscal se encuentra cerrado e inmutable.
    pub fiscal_lock_epoch_ms: i64,
    /// Capas de lotes activas acompañadas de su costo unitario en moneda base `(layer, unit_cost_base)`.
    pub batches: Vec<(ActiveBatchLayer, FixedDecimal128)>, // (layer, unit_cost_base)
    /// Historial cronológico de puntos de control financieros para consultas `last_value` en $O(\log B)$.
    pub checkpoints: Vec<FinancialBlockCheckpoint>,
    /// Cadena secuencial de cabeceras de auditoría criptográfica SHA-256.
    pub audit_headers: Vec<LedgerAuditHeader>,
    /// Hash SHA-256 en la punta actual de la cadena Merkle del libro mayor.
    pub merkle_chain_tip: [u8; 32],
    /// Tabla de tasas de cambio desde códigos de moneda origen hacia la moneda funcional base del libro.
    pub fx_rates_to_base: HashMap<String, FixedDecimal128>,
}

impl FinancialLedgerZone {
    /// Crea una nueva instancia de `FinancialLedgerZone` con la marca de bloqueo fiscal indicada.
    pub fn new(fiscal_lock_epoch_ms: i64) -> Self {
        Self {
            fiscal_lock_epoch_ms,
            batches: Vec::new(),
            checkpoints: Vec::new(),
            audit_headers: Vec::new(),
            merkle_chain_tip: [0u8; 32],
            fx_rates_to_base: HashMap::new(),
        }
    }

    /// Registra una tasa de conversión desde `currency_code` hacia la moneda funcional base del libro mayor.
    pub fn set_exchange_rate(
        &mut self,
        currency_code: impl Into<String>,
        rate_to_base: FixedDecimal128,
    ) {
        self.fx_rates_to_base
            .insert(currency_code.into(), rate_to_base);
    }

    /// Valida un asiento contable de partida doble ($\sum \text{Débitos} == \sum \text{Créditos}$) y lo sella en la cadena Merkle SHA-256.
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

        let prev_block_sha256 = self.merkle_chain_tip;
        let mut hasher = Sha256::new();
        hasher.update(prev_block_sha256);
        hasher.update(tx_epoch_ms.to_le_bytes());
        hasher.update(sum_d.to_le_bytes());
        let digest = hasher.finalize();
        self.merkle_chain_tip.copy_from_slice(&digest);
        self.audit_headers.push(LedgerAuditHeader {
            prev_block_sha256,
            block_merkle_root: self.merkle_chain_tip,
        });
        Ok(self.merkle_chain_tip)
    }

    /// Convierte un importe desde `currency` hacia la moneda funcional base del libro mayor usando la tabla cambiaria configurada.
    pub fn convert_to_base_currency(
        &self,
        amount: FixedDecimal128,
        currency: &str,
    ) -> DomainResult<FixedDecimal128> {
        let rate = self
            .fx_rates_to_base
            .get(currency)
            .copied()
            .ok_or_else(|| {
                DomainError::mbf(
                    ErrorCode::MbfLedgerUnbalanced001,
                    format!("Missing FX rate for currency '{currency}'"),
                )
            })?;
        Ok(amount.mul_bankers(rate))
    }

    /// Recibe una nueva capa de lote y actualiza el punto de control `FinancialBlockCheckpoint` para consultas `last_value`.
    pub fn receive_batch(
        &mut self,
        batch_id: u32,
        resource_row_id: u32,
        received_ms: i64,
        expiry_ms: i64,
        qty: FixedDecimal128,
        unit_cost_base: FixedDecimal128,
    ) -> DomainResult<FinancialBlockCheckpoint> {
        if received_ms <= self.fiscal_lock_epoch_ms {
            return Err(DomainError::mbf(
                ErrorCode::MbfFiscalClosed001,
                "Cannot receive batch in a closed fiscal period",
            ));
        }
        self.batches.push((
            ActiveBatchLayer {
                batch_id,
                resource_row_id,
                received_ms,
                expiry_ms,
                remaining_qty_scaled: qty.0 as i64,
            },
            unit_cost_base,
        ));
        let cp = self.compute_checkpoint(received_ms)?;
        self.checkpoints.push(cp);
        Ok(cp)
    }

    /// Consume o despacha `issue_qty` unidades bajo el método `ValuationMethod` elegido (`FIFO`, `LIFO`, `WAC`, `FEFO`),
    /// devolviendo `(cogs_cost_base, updated_checkpoint)`.
    pub fn issue_inventory(
        &mut self,
        tx_epoch_ms: i64,
        issue_qty: FixedDecimal128,
        method: ValuationMethod,
    ) -> DomainResult<(FixedDecimal128, FinancialBlockCheckpoint)> {
        if tx_epoch_ms <= self.fiscal_lock_epoch_ms {
            return Err(DomainError::mbf(
                ErrorCode::MbfFiscalClosed001,
                "Cannot issue batch in a closed fiscal period",
            ));
        }
        let current_cp = self.compute_checkpoint(tx_epoch_ms)?;
        if current_cp.closing_qty.0 < issue_qty.0 {
            return Err(DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "Insufficient stock to satisfy consumption",
            ));
        }

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

    /// Resuelve la agregación `last_value` al corte `target_epoch_ms` en tiempo $O(\log B)$ mediante búsqueda binaria sobre los puntos de control.
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

    /// Calcula la amortización lineal proporcional al uso ($\frac{\text{Cost} - \text{Salvage}}{\text{TotalCapacity}} \times \text{ConsumedUnits}$) aplicando redondeo bancario.
    pub fn compute_straight_line_amortization(
        acquisition_cost: FixedDecimal128,
        salvage_value: FixedDecimal128,
        total_capacity_units: u64,
        consumed_units: u64,
    ) -> DomainResult<FixedDecimal128> {
        if total_capacity_units == 0 {
            return Err(DomainError::mbf(
                ErrorCode::MbfLedgerUnbalanced001,
                "Total capacity units cannot be zero in straight-line amortization",
            ));
        }
        let amortizable_base = acquisition_cost.checked_sub(salvage_value)?;
        let capped_units = consumed_units.min(total_capacity_units);
        let ratio = FixedDecimal128::from_whole(capped_units as i64)
            .div_bankers(FixedDecimal128::from_whole(total_capacity_units as i64))?;
        Ok(amortizable_base.mul_bankers(ratio))
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

    /// Codifica la Zona 4 en un bloque binario alineado a 64 bytes:
    /// `` `[Magic "MBFZ4FIN" (8B) | fiscal_lock_epoch_ms | merkle_chain_tip | ActiveBatchLayers (32B) | FinancialBlockCheckpoints (64B) | LedgerAuditHeaders (64B) | EmbeddedFxMatrix | Padding 64B]` ``.
    pub fn encode_zone_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ZONE4_MAGIC);
        buf.extend_from_slice(&self.fiscal_lock_epoch_ms.to_le_bytes());
        buf.extend_from_slice(&self.merkle_chain_tip);

        // 1. ActiveBatchLayers (32B each) + unit_cost (16B)
        buf.extend_from_slice(&(self.batches.len() as u32).to_le_bytes());
        for &(layer, cost) in &self.batches {
            buf.extend_from_slice(bytemuck::bytes_of(&layer));
            buf.extend_from_slice(&cost.0.to_le_bytes());
        }

        // 2. FinancialBlockCheckpoints (64B each)
        buf.extend_from_slice(&(self.checkpoints.len() as u32).to_le_bytes());
        for cp in &self.checkpoints {
            buf.extend_from_slice(bytemuck::bytes_of(cp));
        }

        // 3. LedgerAuditHeaders (64B each)
        buf.extend_from_slice(&(self.audit_headers.len() as u32).to_le_bytes());
        for ah in &self.audit_headers {
            buf.extend_from_slice(bytemuck::bytes_of(ah));
        }

        // 4. EmbeddedFxMatrix
        let mut sorted_fx: Vec<(&String, &FixedDecimal128)> =
            self.fx_rates_to_base.iter().collect();
        sorted_fx.sort_by(|a, b| a.0.cmp(b.0));
        buf.extend_from_slice(&(sorted_fx.len() as u32).to_le_bytes());
        for (curr, rate) in sorted_fx {
            let cb = curr.as_bytes();
            buf.extend_from_slice(&(cb.len() as u16).to_le_bytes());
            buf.extend_from_slice(cb);
            buf.extend_from_slice(&rate.0.to_le_bytes());
        }

        pad_to_64(&mut buf);
        buf
    }

    /// Decodifica una instancia de `FinancialLedgerZone` de la Zona 4 a partir de su bloque binario alineado a 64 bytes.
    pub fn decode_zone_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 48 || bytes[0..8] != ZONE4_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid Zone 4 Financial Ledger binary magic header",
            ));
        }
        let mut cur = 8usize;
        let fiscal_lock_epoch_ms = read_i64(bytes, &mut cur)?;
        let mut merkle_chain_tip = [0u8; 32];
        merkle_chain_tip.copy_from_slice(&bytes[cur..cur + 32]);
        cur += 32;

        let batch_count = read_u32(bytes, &mut cur)? as usize;
        let mut batches = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            if cur + 48 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Unexpected EOF reading ActiveBatchLayer in Zone 4",
                ));
            }
            let layer: ActiveBatchLayer = bytemuck::pod_read_unaligned(&bytes[cur..cur + 32]);
            cur += 32;
            let mut cost_arr = [0u8; 16];
            cost_arr.copy_from_slice(&bytes[cur..cur + 16]);
            cur += 16;
            batches.push((layer, FixedDecimal128(i128::from_le_bytes(cost_arr))));
        }

        let cp_count = read_u32(bytes, &mut cur)? as usize;
        let mut checkpoints = Vec::with_capacity(cp_count);
        for _ in 0..cp_count {
            if cur + 64 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Unexpected EOF reading FinancialBlockCheckpoint in Zone 4",
                ));
            }
            let cp: FinancialBlockCheckpoint = bytemuck::pod_read_unaligned(&bytes[cur..cur + 64]);
            cur += 64;
            checkpoints.push(cp);
        }

        let ah_count = read_u32(bytes, &mut cur)? as usize;
        let mut audit_headers = Vec::with_capacity(ah_count);
        for _ in 0..ah_count {
            if cur + 64 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Unexpected EOF reading LedgerAuditHeader in Zone 4",
                ));
            }
            let ah: LedgerAuditHeader = bytemuck::pod_read_unaligned(&bytes[cur..cur + 64]);
            cur += 64;
            audit_headers.push(ah);
        }

        let fx_count = read_u32(bytes, &mut cur)? as usize;
        let mut fx_rates_to_base = HashMap::with_capacity(fx_count);
        for _ in 0..fx_count {
            if cur + 2 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Unexpected EOF reading FX currency length in Zone 4",
                ));
            }
            let clen = u16::from_le_bytes([bytes[cur], bytes[cur + 1]]) as usize;
            cur += 2;
            if cur + clen + 16 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Unexpected EOF reading FX entry in Zone 4",
                ));
            }
            let curr = String::from_utf8_lossy(&bytes[cur..cur + clen]).into_owned();
            cur += clen;
            let mut rate_arr = [0u8; 16];
            rate_arr.copy_from_slice(&bytes[cur..cur + 16]);
            cur += 16;
            fx_rates_to_base.insert(curr, FixedDecimal128(i128::from_le_bytes(rate_arr)));
        }

        Ok(Self {
            fiscal_lock_epoch_ms,
            batches,
            checkpoints,
            audit_headers,
            merkle_chain_tip,
            fx_rates_to_base,
        })
    }
}

fn read_u32(bytes: &[u8], cur: &mut usize) -> DomainResult<u32> {
    if *cur + 4 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading u32 in Zone 4",
        ));
    }
    let v = u32::from_le_bytes([
        bytes[*cur],
        bytes[*cur + 1],
        bytes[*cur + 2],
        bytes[*cur + 3],
    ]);
    *cur += 4;
    Ok(v)
}

fn read_i64(bytes: &[u8], cur: &mut usize) -> DomainResult<i64> {
    if *cur + 8 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading i64 in Zone 4",
        ));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[*cur..*cur + 8]);
    *cur += 8;
    Ok(i64::from_le_bytes(arr))
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
        assert_eq!(std::mem::size_of::<LedgerAuditHeader>(), 64);
        assert_eq!(std::mem::align_of::<LedgerAuditHeader>(), 64);

        let total = FixedDecimal128::from_whole(100);
        let parts = FixedDecimal128::allocate_hamilton(total, &[1, 1, 1]).unwrap();
        let sum: i128 = parts.iter().map(|p| p.0).sum();
        assert_eq!(sum, total.0);
        assert_eq!(parts[0].0, 33_3334);
        assert_eq!(parts[1].0, 33_3333);
        assert_eq!(parts[2].0, 33_3333);
    }

    #[test]
    fn fifo_lifo_fefo_wac_batch_valuation_fiscal_lock_and_binary_roundtrip() {
        let mut ledger = FinancialLedgerZone::new(1_000);
        ledger.set_exchange_rate("BASE", FixedDecimal128::from_whole(1));
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
        ledger
            .record_journal_entry(
                3_500,
                &[FixedDecimal128::from_whole(3000)],
                &[FixedDecimal128::from_whole(3000)],
            )
            .unwrap();

        // Binary roundtrip test
        let bin = ledger.encode_zone_binary();
        assert_eq!(bin.len() % 64, 0);
        assert_eq!(&bin[0..8], b"MBFZ4FIN");
        let restored = FinancialLedgerZone::decode_zone_binary(&bin).unwrap();
        assert_eq!(restored, ledger);

        // Issue 5 units under FIFO -> must take from Batch 1 @ 100 = 500
        let mut fifo_ledger = restored.clone();
        let (fifo_cogs, _) = fifo_ledger
            .issue_inventory(4_000, FixedDecimal128::from_whole(5), ValuationMethod::Fifo)
            .unwrap();
        assert_eq!(fifo_cogs, FixedDecimal128::from_whole(500));

        // Issue 5 units under LIFO -> must take from Batch 2 @ 200 = 1000
        let mut lifo_ledger = restored.clone();
        let (lifo_cogs, _) = lifo_ledger
            .issue_inventory(4_000, FixedDecimal128::from_whole(5), ValuationMethod::Lifo)
            .unwrap();
        assert_eq!(lifo_cogs, FixedDecimal128::from_whole(1000));

        // Issue 5 units under FEFO -> Batch 2 expires at 5000 < 9000, so takes Batch 2 @ 200 = 1000
        let mut fefo_ledger = restored.clone();
        let (fefo_cogs, _) = fefo_ledger
            .issue_inventory(4_000, FixedDecimal128::from_whole(5), ValuationMethod::Fefo)
            .unwrap();
        assert_eq!(fefo_cogs, FixedDecimal128::from_whole(1000));

        // Issue 5 units under WAC (PMP) -> Average unit cost is (1000 + 2000)/20 = 150 -> 5 * 150 = 750
        let mut wac_ledger = restored.clone();
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
