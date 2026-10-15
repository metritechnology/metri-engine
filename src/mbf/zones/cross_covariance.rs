//! Zone 6B multivariate cross-covariance matrix for O(1) Pearson correlation, multiple OLS regression, PCA, and Mahalanobis distance.
#![allow(clippy::needless_range_loop)]

use serde::{Deserialize, Serialize};

/// Parallel-mergeable multivariate co-moment matrix ($M \times M$) stored in Zone 6B of `.mbf`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MbfCrossCovarianceMatrix {
    pub dimension: usize,
    pub count: u64,
    pub means: Vec<f64>,
    /// Flattened $M \times M$ symmetric matrix of centered co-moments $C_{i,j} = \sum_{k=1}^n (x_{i,k} - \mu_i)(x_{j,k} - \mu_j)$.
    pub co_moments: Vec<f64>,
}

impl MbfCrossCovarianceMatrix {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            count: 0,
            means: vec![0.0; dimension],
            co_moments: vec![0.0; dimension * dimension],
        }
    }

    /// Updates the $M \times M$ co-moment matrix with a single multivariate observation row in $O(M^2)$.
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

    /// Merges two `MbfCrossCovarianceMatrix` blocks in $O(M^2)$ without scanning underlying rows!
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

    /// Returns the full $M \times M$ Pearson Correlation Matrix $R_{i,j} \in [-1, 1]$ in $O(1)$.
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

    /// Solves Multiple Linear Regression (OLS) directly from the pre-aggregated co-moment matrix in $O(M^3)$
    /// without reading a single data row! Predicts target column `target_idx` from `predictor_indices`.
    /// Returns `(intercept_beta0, slopes_beta, r_squared)`.
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
                    // Ridge regularization epsilon for numerical stability
                    a[row_i][col_j] += 1e-9;
                }
            }
        }

        let betas = Self::solve_linear_system(a, b)?;
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

    /// Extracts the First Principal Component (`PC1` eigenvector and explained variance ratio) via Power Iteration.
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

    /// Computes the Multivariate Mahalanobis Distance $D_M(x) = \sqrt{(x - \mu)^T \Sigma^{-1} (x - \mu)}$ for anomaly detection.
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
        let inv_diff = Self::solve_linear_system(cov, diff.clone())?;
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
    }
}
