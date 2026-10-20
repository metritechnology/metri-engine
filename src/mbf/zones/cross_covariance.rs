//! Matriz de covarianza cruzada multivariante de la Zona 6B para correlación de Pearson en $O(1)$, regresión OLS múltiple, PCA y distancia de Mahalanobis.
#![allow(clippy::needless_range_loop)]

use serde::{Deserialize, Serialize};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::pad_to_64;

/// Bytes mágicos binarios de la Zona 6B (`"MBFZ6BCV"`).
pub const MBF_ZONE6B_MAGIC: [u8; 8] = *b"MBFZ6BCV";

/// Matriz de co-momentos multivariantes ($M \times M$) fusionable en paralelo almacenada en la Zona 6B de `.mbf`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MbfCrossCovarianceMatrix {
    /// Dimensión $M$ de variables numéricas rastreadas.
    pub dimension: usize,
    /// Número total de observaciones multivariantes acumuladas.
    pub count: u64,
    /// Vector de medias muestrales $\mu_i$ para cada dimensión.
    pub means: Vec<f64>,
    /// Matriz simétrica aplanada $M \times M$ de co-momentos centrados $C_{i,j} = \sum_{k=1}^n (x_{i,k} - \mu_i)(x_{j,k} - \mu_j)$.
    pub co_moments: Vec<f64>,
}

impl MbfCrossCovarianceMatrix {
    /// Crea una matriz de covarianza cruzada vacía de dimensión `dimension`.
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            count: 0,
            means: vec![0.0; dimension],
            co_moments: vec![0.0; dimension * dimension],
        }
    }

    /// Codifica la Zona 6B en un búfer binario alineado a 64 bytes con `dimension`, `count`, `means` y la triangular superior de co-momentos.
    pub fn encode_zone_binary(&self) -> Vec<u8> {
        let m = self.dimension;
        let upper_len = m * (m + 1) / 2;
        let mut buf = Vec::with_capacity(64 + (m + upper_len) * 8);
        buf.extend_from_slice(&MBF_ZONE6B_MAGIC);
        buf.extend_from_slice(&(m as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&self.count.to_le_bytes());
        for &mu in &self.means {
            buf.extend_from_slice(&mu.to_bits().to_le_bytes());
        }
        for i in 0..m {
            for j in i..m {
                let c_ij = self.co_moments.get(i * m + j).copied().unwrap_or(0.0);
                buf.extend_from_slice(&c_ij.to_bits().to_le_bytes());
            }
        }
        pad_to_64(&mut buf);
        buf
    }

    /// Decodifica un búfer binario de la Zona 6B alineado a 64 bytes en una instancia de `MbfCrossCovarianceMatrix`.
    pub fn decode_zone_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 24 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Zone 6B buffer too small: {} bytes", bytes.len()),
            ));
        }
        if bytes[0..8] != MBF_ZONE6B_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid Zone 6B magic (expected MBFZ6BCV)",
            ));
        }
        let mut u32_buf = [0u8; 4];
        u32_buf.copy_from_slice(&bytes[8..12]);
        let m = u32::from_le_bytes(u32_buf) as usize;
        let mut u64_buf = [0u8; 8];
        u64_buf.copy_from_slice(&bytes[16..24]);
        let count = u64::from_le_bytes(u64_buf);

        let upper_len = m * (m + 1) / 2;
        let needed = 24 + (m + upper_len) * 8;
        if bytes.len() < needed {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated Zone 6B co-moments payload",
            ));
        }

        let mut offset = 24usize;
        let mut means = Vec::with_capacity(m);
        for _ in 0..m {
            u64_buf.copy_from_slice(&bytes[offset..offset + 8]);
            offset += 8;
            means.push(f64::from_bits(u64::from_le_bytes(u64_buf)));
        }

        let mut co_moments = vec![0.0_f64; m * m];
        for i in 0..m {
            for j in i..m {
                u64_buf.copy_from_slice(&bytes[offset..offset + 8]);
                offset += 8;
                let val = f64::from_bits(u64::from_le_bytes(u64_buf));
                co_moments[i * m + j] = val;
                co_moments[j * m + i] = val;
            }
        }

        Ok(Self {
            dimension: m,
            count,
            means,
            co_moments,
        })
    }

    /// Actualiza la matriz de co-momentos $M \times M$ con una observación multivariante en $O(M^2)$.
    pub fn push_row(&mut self, row: &[f64]) {
        if row.len() != self.dimension {
            return;
        }
        self.count += 1;
        let n = self.count as f64;
        let mut deltas_old = vec![0.0; self.dimension];
        let mut deltas_new = vec![0.0; self.dimension];

        for i in 0..self.dimension {
            deltas_old[i] = row[i] - self.means[i];
            self.means[i] += deltas_old[i] / n;
            deltas_new[i] = row[i] - self.means[i];
        }

        for i in 0..self.dimension {
            for j in i..self.dimension {
                let delta_c = deltas_old[i] * deltas_new[j];
                self.co_moments[i * self.dimension + j] += delta_c;
                if i != j {
                    self.co_moments[j * self.dimension + i] =
                        self.co_moments[i * self.dimension + j];
                }
            }
        }
    }

    /// Fusiona dos bloques `MbfCrossCovarianceMatrix` en $O(M^2)$ sin reescanear las filas originales.
    pub fn merge(&self, other: &Self) -> Self {
        if self.count == 0 {
            return other.clone();
        }
        if other.count == 0 || self.dimension != other.dimension {
            return self.clone();
        }
        let m = self.dimension;
        let na = self.count as f64;
        let nb = other.count as f64;
        let nab = na + nb;
        let scale = (na * nb) / nab;

        let mut out = Self::new(m);
        out.count = self.count + other.count;
        let mut deltas = vec![0.0; m];
        for i in 0..m {
            deltas[i] = other.means[i] - self.means[i];
            out.means[i] = self.means[i] + deltas[i] * (nb / nab);
        }
        for i in 0..m {
            for j in 0..m {
                let idx = i * m + j;
                out.co_moments[idx] =
                    self.co_moments[idx] + other.co_moments[idx] + deltas[i] * deltas[j] * scale;
            }
        }
        out
    }

    /// Calcula la matriz completa $M \times M$ de correlación de Pearson $R_{i,j}$ en `` `[-1, 1]` `` en $O(1)$.
    pub fn pearson_correlation_matrix(&self) -> Vec<Vec<f64>> {
        let m = self.dimension;
        let mut r = vec![vec![0.0; m]; m];
        for i in 0..m {
            for j in 0..m {
                if i == j {
                    r[i][j] = 1.0;
                } else {
                    let cii = self.co_moments[i * m + i];
                    let cjj = self.co_moments[j * m + j];
                    let denom = (cii * cjj).sqrt();
                    if denom > 1e-15 {
                        r[i][j] = (self.co_moments[i * m + j] / denom).clamp(-1.0, 1.0);
                    }
                }
            }
        }
        r
    }

    /// Calcula la matriz $M \times M$ de correlación por rangos de Spearman $\rho_S$ en `` `[-1, 1]` `` en $O(1)$ mediante la identidad analítica de Kruskal
    /// $\rho_S = \frac{6}{\pi} \arcsin(R_{i,j} / 2)$ a partir de la matriz de co-momentos preagregada.
    pub fn spearman_correlation_matrix(&self) -> Vec<Vec<f64>> {
        let pearson = self.pearson_correlation_matrix();
        let m = self.dimension;
        let mut s = vec![vec![0.0; m]; m];
        for i in 0..m {
            for j in 0..m {
                if i == j {
                    s[i][j] = 1.0;
                } else {
                    let r_ij = pearson[i][j].clamp(-1.0, 1.0);
                    s[i][j] = ((6.0 / std::f64::consts::PI) * (0.5 * r_ij).asin()).clamp(-1.0, 1.0);
                }
            }
        }
        s
    }

    /// Calcula la matriz empírica exacta de correlación de Spearman transformando cada columna en rangos sobre una `MbfCrossCovarianceMatrix`.
    pub fn spearman_from_rows(rows: &[Vec<f64>]) -> Vec<Vec<f64>> {
        if rows.is_empty() {
            return Vec::new();
        }
        let m = rows[0].len();
        let n = rows.len();
        let mut ranked_rows = vec![vec![0.0; m]; n];
        for col in 0..m {
            let mut indexed: Vec<(usize, f64)> = rows
                .iter()
                .enumerate()
                .map(|(r_idx, r)| (r_idx, r.get(col).copied().unwrap_or(0.0)))
                .collect();
            indexed.sort_by(|a, b| a.1.total_cmp(&b.1));
            for (rank, (orig_idx, _)) in indexed.into_iter().enumerate() {
                ranked_rows[orig_idx][col] = (rank + 1) as f64;
            }
        }
        let mut mat = Self::new(m);
        for r in &ranked_rows {
            mat.push_row(r);
        }
        mat.pearson_correlation_matrix()
    }

    /// Calcula la correlación bivariante exacta de Pearson $r_{XY} = \frac{C_{XY}}{\sqrt{M_{2,X} M_{2,Y}}}$ en `` `[-1, 1]` `` en $O(1)$
    /// directamente a partir de los co-momentos preagregados.
    #[inline]
    pub fn bivariate_correlation(&self, idx_x: usize, idx_y: usize) -> f64 {
        let m = self.dimension;
        if idx_x >= m || idx_y >= m || self.count < 2 {
            return 0.0;
        }
        if idx_x == idx_y {
            return 1.0;
        }
        let m2_x = self.co_moments[idx_x * m + idx_x];
        let m2_y = self.co_moments[idx_y * m + idx_y];
        let denom = (m2_x * m2_y).sqrt();
        if denom <= 1e-15 {
            0.0
        } else {
            (self.co_moments[idx_x * m + idx_y] / denom).clamp(-1.0, 1.0)
        }
    }

    /// Resuelve la regresión lineal simple por mínimos cuadrados ordinarios ($y = m x + b$) en $O(1)$:
    /// $$m = \frac{C_{XY}}{M_{2,X}}, \qquad b = \mu_Y - m \cdot \mu_X$$
    /// Devuelve `Some((slope_m, intercept_b, r_xy))`.
    #[inline]
    pub fn simple_linear_regression(&self, idx_x: usize, idx_y: usize) -> Option<(f64, f64, f64)> {
        let m = self.dimension;
        if idx_x >= m || idx_y >= m || self.count < 2 {
            return None;
        }
        let m2_x = self.co_moments[idx_x * m + idx_x];
        if m2_x.abs() <= 1e-15 {
            return None;
        }
        let c_xy = self.co_moments[idx_x * m + idx_y];
        let slope_m = c_xy / m2_x;
        let intercept_b = self.means[idx_y] - slope_m * self.means[idx_x];
        let r_xy = self.bivariate_correlation(idx_x, idx_y);
        Some((slope_m, intercept_b, r_xy))
    }

    /// Resuelve la regresión lineal múltiple (OLS) directamente desde la matriz de co-momentos en $O(M^3)$
    /// mediante descomposición de Cholesky $L L^T$ sin leer filas de datos.
    /// Predice la columna `target_idx` a partir de `predictor_indices` y devuelve `(intercept_beta0, slopes_beta, r_squared)`.
    pub fn solve_multiple_ols(
        &self,
        predictor_indices: &[usize],
        target_idx: usize,
    ) -> Option<(f64, Vec<f64>, f64)> {
        let k = predictor_indices.len();
        if k == 0 || target_idx >= self.dimension || self.count <= (k as u64) {
            return None;
        }
        let m = self.dimension;
        let mut a = vec![vec![0.0; k]; k];
        let mut b = vec![0.0; k];

        for (row_i, &pi) in predictor_indices.iter().enumerate() {
            if pi >= m {
                return None;
            }
            b[row_i] = self.co_moments[pi * m + target_idx];
            for (col_j, &pj) in predictor_indices.iter().enumerate() {
                a[row_i][col_j] = self.co_moments[pi * m + pj];
                if row_i == col_j {
                    // Ridge regularization epsilon for positive-definiteness
                    a[row_i][col_j] += 1e-9;
                }
            }
        }

        let betas = Self::solve_cholesky(&a, &b).or_else(|| Self::solve_linear_system(a, b))?;
        let mut beta0 = self.means[target_idx];
        let mut explained_ss = 0.0;
        for (i, &pi) in predictor_indices.iter().enumerate() {
            beta0 -= betas[i] * self.means[pi];
            explained_ss += betas[i] * self.co_moments[pi * m + target_idx];
        }
        let total_ss = self.co_moments[target_idx * m + target_idx];
        let r2 = if total_ss > 1e-15 {
            (explained_ss / total_ss).clamp(0.0, 1.0)
        } else {
            1.0
        };
        Some((beta0, betas, r2))
    }

    /// Resuelve un sistema lineal simétrico definido positivo $A \beta = b$ mediante descomposición de Cholesky $L L^T$.
    pub fn solve_cholesky(a: &[Vec<f64>], b: &[f64]) -> Option<Vec<f64>> {
        let n = b.len();
        if n == 0 || a.len() != n {
            return None;
        }
        let mut l = vec![vec![0.0; n]; n];
        for i in 0..n {
            for j in 0..=i {
                let mut sum = 0.0;
                for p in 0..j {
                    sum += l[i][p] * l[j][p];
                }
                if i == j {
                    let diag = a[i][i] - sum;
                    if diag <= 1e-15 {
                        return None;
                    }
                    l[i][j] = diag.sqrt();
                } else {
                    l[i][j] = (a[i][j] - sum) / l[j][j];
                }
            }
        }
        // Forward substitution: L y = b
        let mut y = vec![0.0; n];
        for i in 0..n {
            let mut sum = b[i];
            for j in 0..i {
                sum -= l[i][j] * y[j];
            }
            y[i] = sum / l[i][i];
        }
        // Backward substitution: L^T x = y
        let mut x = vec![0.0; n];
        for i in (0..n).rev() {
            let mut sum = y[i];
            for j in (i + 1)..n {
                sum -= l[j][i] * x[j];
            }
            x[i] = sum / l[i][i];
        }
        Some(x)
    }

    /// Extrae el primer componente principal (`PC1`, autovector y proporción de varianza explicada) mediante iteración de potencias.
    pub fn first_principal_component(&self) -> (Vec<f64>, f64) {
        let m = self.dimension;
        if m == 0 || self.count < 2 {
            return (Vec::new(), 0.0);
        }
        let mut v = vec![1.0 / (m as f64).sqrt(); m];
        for _ in 0..24 {
            let mut next_v = vec![0.0; m];
            for i in 0..m {
                for j in 0..m {
                    next_v[i] += self.co_moments[i * m + j] * v[j];
                }
            }
            let norm = next_v.iter().map(|x| x * x).sum::<f64>().sqrt();
            if norm <= 1e-15 {
                break;
            }
            for x in &mut next_v {
                *x /= norm;
            }
            v = next_v;
        }
        // Rayleigh quotient for eigenvalue lambda_1
        let mut lambda1 = 0.0;
        let mut trace = 0.0;
        for i in 0..m {
            trace += self.co_moments[i * m + i];
            let mut av_i = 0.0;
            for j in 0..m {
                av_i += self.co_moments[i * m + j] * v[j];
            }
            lambda1 += v[i] * av_i;
        }
        let explained_ratio = if trace > 1e-15 {
            (lambda1 / trace).clamp(0.0, 1.0)
        } else {
            0.0
        };
        (v, explained_ratio)
    }

    /// Calcula la distancia multivariante de Mahalanobis $D_M(x) = \sqrt{(x - \mu)^T \Sigma^{-1} (x - \mu)}$ para detección de anomalías.
    pub fn mahalanobis_distance(&self, point: &[f64]) -> Option<f64> {
        let m = self.dimension;
        if point.len() != m || self.count <= (m as u64) {
            return None;
        }
        let dof = (self.count - 1) as f64;
        let mut cov = vec![vec![0.0; m]; m];
        let mut diff = vec![0.0; m];
        for i in 0..m {
            diff[i] = point[i] - self.means[i];
            for j in 0..m {
                cov[i][j] = self.co_moments[i * m + j] / dof;
                if i == j {
                    cov[i][j] += 1e-6;
                }
            }
        }
        let inv_diff = Self::solve_cholesky(&cov, &diff)
            .or_else(|| Self::solve_linear_system(cov, diff.clone()))?;
        let d2: f64 = diff
            .iter()
            .zip(inv_diff.iter())
            .map(|(a, b)| a * b)
            .sum::<f64>()
            .max(0.0);
        Some(d2.sqrt())
    }

    fn solve_linear_system(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Option<Vec<f64>> {
        let n = b.len();
        for i in 0..n {
            let mut pivot = i;
            for r in (i + 1)..n {
                if a[r][i].abs() > a[pivot][i].abs() {
                    pivot = r;
                }
            }
            if a[pivot][i].abs() < 1e-14 {
                return None;
            }
            a.swap(i, pivot);
            b.swap(i, pivot);

            let diag = a[i][i];
            for c in i..n {
                a[i][c] /= diag;
            }
            b[i] /= diag;

            for r in 0..n {
                if r != i {
                    let factor = a[r][i];
                    for c in i..n {
                        a[r][c] -= factor * a[i][c];
                    }
                    b[r] -= factor * b[i];
                }
            }
        }
        Some(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multivariate_ols_pca_and_mahalanobis_work_in_o1_from_merged_co_moments() {
        // True relation: total_cost (y) = 50.0 + 3.0 * hours (x1) + 2.0 * parts (x2)
        let mut block1 = MbfCrossCovarianceMatrix::new(3);
        let mut block2 = MbfCrossCovarianceMatrix::new(3);

        for i in 1..=30 {
            let x1 = i as f64;
            let x2 = ((i * 7) % 19) as f64;
            let y = 50.0 + 3.0 * x1 + 2.0 * x2;
            if i <= 15 {
                block1.push_row(&[x1, x2, y]);
            } else {
                block2.push_row(&[x1, x2, y]);
            }
        }

        let merged = block1.merge(&block2);
        let (beta0, betas, r2) = merged.solve_multiple_ols(&[0, 1], 2).unwrap();
        assert!((beta0 - 50.0).abs() < 1e-5);
        assert!((betas[0] - 3.0).abs() < 1e-5);
        assert!((betas[1] - 2.0).abs() < 1e-5);
        assert!((r2 - 1.0).abs() < 1e-6);

        let corr = merged.pearson_correlation_matrix();
        assert_eq!(corr.len(), 3);
        assert!((corr[0][0] - 1.0).abs() < 1e-12);

        let (pc1, ratio) = merged.first_principal_component();
        assert_eq!(pc1.len(), 3);
        assert!(ratio > 0.70);

        let normal_dist = merged.mahalanobis_distance(&merged.means).unwrap();
        assert!(normal_dist < 1e-6);
        let anomaly_dist = merged
            .mahalanobis_distance(&[500.0, 500.0, -1000.0])
            .unwrap();
        assert!(anomaly_dist > 10.0);

        let bin = merged.encode_zone_binary();
        assert_eq!(bin.len() % 64, 0);
        let decoded = MbfCrossCovarianceMatrix::decode_zone_binary(&bin).unwrap();
        assert_eq!(decoded, merged);

        // Verify O(1) Bivariate Simple Linear Regression (§2.2): y = 12.5 + 4.0 * x
        let mut biv = MbfCrossCovarianceMatrix::new(2);
        for x in 1..=20 {
            let xf = x as f64;
            biv.push_row(&[xf, 12.5 + 4.0 * xf]);
        }
        let (slope_m, intercept_b, r_xy) = biv.simple_linear_regression(0, 1).unwrap();
        assert!((slope_m - 4.0).abs() < 1e-10);
        assert!((intercept_b - 12.5).abs() < 1e-10);
        assert!((r_xy - 1.0).abs() < 1e-10);
    }
}
