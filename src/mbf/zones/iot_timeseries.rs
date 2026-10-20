//! Compresión de series temporales de alta frecuencia de la Zona 3 mediante empaquetado de bits Delta-of-Delta, tramas Chimp128 + LZ4, normalización dimensional afín y pirámides multirresolución (`1m`, `1h`, `1d`).

use serde::{Deserialize, Serialize};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::pad_to_64;
use crate::mbf::zones::fuzzy_math_catalog::AffineTransform;

const ZONE3_MAGIC: [u8; 8] = *b"MBFZ3IOT";

/// Cubeta de agregación precalculada `(bucket_ms, count, sum_canonical, min_canonical, max_canonical, first_canonical, last_canonical)`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PyramidRollupBucket {
    /// Marca de tiempo de inicio de la ventana en milisegundos UTC.
    pub bucket_ms: i64,
    /// Cantidad de lecturas contenidas en la cubeta.
    pub count: u64,
    /// Suma acumulada de valores normalizados a la escala canónica.
    pub sum_si: f64,
    /// Valor mínimo observado en la escala canónica.
    pub min_si: f64,
    /// Valor máximo observado en la escala canónica.
    pub max_si: f64,
    /// Primer valor cronológico observado en la cubeta.
    pub first_si: f64,
    /// Último valor cronológico observado en la cubeta.
    pub last_si: f64,
}

/// Flujo de serie temporal comprimido con Delta-of-Delta + Chimp128 XOR + LZ4 para métricas de alta frecuencia en la Zona 3.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Chimp128SeriesBlock {
    /// Identificador de fila de la entidad o sujeto emisor.
    pub subject_row_id: u32,
    /// Identificador numérico del atributo de métrica.
    pub metric_attr_id: u16,
    /// Código simbólico de la métrica registrada.
    pub metric_code: String,
    /// Cantidad total de muestras almacenadas en el bloque.
    pub count: usize,
    /// Marca de tiempo base ($t_0$) en milisegundos UTC.
    pub base_timestamp_ms: i64,
    /// Primer delta temporal ($\Delta_1 = t_1 - t_0$) en milisegundos.
    pub first_delta_ms: i64,
    /// Secuencia de segundas diferencias temporales ($\Delta^2_i = \Delta_i - \Delta_{i-1}$).
    pub delta_of_deltas_ms: Vec<i64>,
    /// Representación IEEE-754 en bits de la primera lectura canónica ($v_0$).
    pub base_value_bits: u64,
    /// Residuos XOR sucesivos entre lecturas IEEE-754 consecutivas.
    pub xor_residuals: Vec<u64>,
    /// Cubetas preagregadas con resolución de 1 minuto (`1m`).
    pub minute_pyramid: Vec<PyramidRollupBucket>,
    /// Cubetas preagregadas con resolución de 1 hora `(hour_bucket_ms, count, sum_canonical, min_canonical, max_canonical, last_canonical)`.
    pub hourly_pyramid: Vec<(i64, u64, f64, f64, f64, f64)>,
    /// Cubetas preagregadas con resolución de 1 día (`1d`).
    pub daily_pyramid: Vec<PyramidRollupBucket>,
}

impl Chimp128SeriesBlock {
    /// Crea un nuevo bloque de serie temporal `Chimp128SeriesBlock` para el sujeto y código de métrica indicados.
    pub fn new(subject_row_id: u32, metric_code: impl Into<String>) -> Self {
        Self {
            subject_row_id,
            metric_attr_id: 1,
            metric_code: metric_code.into(),
            count: 0,
            base_timestamp_ms: 0,
            first_delta_ms: 0,
            delta_of_deltas_ms: Vec::new(),
            base_value_bits: 0,
            xor_residuals: Vec::new(),
            minute_pyramid: Vec::new(),
            hourly_pyramid: Vec::new(),
            daily_pyramid: Vec::new(),
        }
    }

    /// Añade una muestra `(timestamp_ms, raw_value, affine_transform)`, normalizándola a escala canónica ($y = a \cdot x + b$),
    /// comprimiéndola mediante Delta-of-Delta + residuos XOR y actualizando las pirámides `1m`, `1h` y `1d`.
    pub fn push_reading(&mut self, timestamp_ms: i64, raw_value: f64, transform: AffineTransform) {
        let canonical_val = transform.apply(raw_value);
        let val_bits = canonical_val.to_bits();

        if self.count == 0 {
            self.base_timestamp_ms = timestamp_ms;
            self.base_value_bits = val_bits;
        } else if self.count == 1 {
            self.first_delta_ms = timestamp_ms - self.base_timestamp_ms;
            let prev_bits = self.base_value_bits;
            self.xor_residuals.push(val_bits ^ prev_bits);
        } else {
            let mut prev_t = self.base_timestamp_ms + self.first_delta_ms;
            let mut prev_delta = self.first_delta_ms;
            for &d2 in &self.delta_of_deltas_ms {
                prev_delta += d2;
                prev_t += prev_delta;
            }
            let current_delta = timestamp_ms - prev_t;
            self.delta_of_deltas_ms.push(current_delta - prev_delta);

            let mut prev_bits = self.base_value_bits;
            for &xr in &self.xor_residuals {
                prev_bits ^= xr;
            }
            self.xor_residuals.push(val_bits ^ prev_bits);
        }
        self.count += 1;

        // 1. Update 1-minute pyramid (60,000 ms)
        Self::update_pyramid_vec(
            &mut self.minute_pyramid,
            (timestamp_ms / 60_000) * 60_000,
            canonical_val,
        );

        // 2. Update 1-hour pyramid (3,600,000 ms)
        let hour_bucket = (timestamp_ms / 3_600_000) * 3_600_000;
        let updated_hour = if let Some(last_bucket) = self.hourly_pyramid.last_mut() {
            if last_bucket.0 == hour_bucket {
                last_bucket.1 += 1;
                last_bucket.2 += canonical_val;
                if canonical_val < last_bucket.3 {
                    last_bucket.3 = canonical_val;
                }
                if canonical_val > last_bucket.4 {
                    last_bucket.4 = canonical_val;
                }
                last_bucket.5 = canonical_val;
                true
            } else {
                false
            }
        } else {
            false
        };
        if !updated_hour {
            self.hourly_pyramid.push((
                hour_bucket,
                1,
                canonical_val,
                canonical_val,
                canonical_val,
                canonical_val,
            ));
        }

        // 3. Update 1-day pyramid (86,400,000 ms)
        Self::update_pyramid_vec(
            &mut self.daily_pyramid,
            (timestamp_ms / 86_400_000) * 86_400_000,
            canonical_val,
        );
    }

    fn update_pyramid_vec(pyramid: &mut Vec<PyramidRollupBucket>, bucket_ms: i64, val: f64) {
        if let Some(last) = pyramid.last_mut() {
            if last.bucket_ms == bucket_ms {
                last.count += 1;
                last.sum_si += val;
                if val < last.min_si {
                    last.min_si = val;
                }
                if val > last.max_si {
                    last.max_si = val;
                }
                last.last_si = val;
                return;
            }
        }
        pyramid.push(PyramidRollupBucket {
            bucket_ms,
            count: 1,
            sum_si: val,
            min_si: val,
            max_si: val,
            first_si: val,
            last_si: val,
        });
    }

    /// Descomprime sin pérdida todas las lecturas `(timestamp_ms, canonical_value)`.
    pub fn decompress_all(&self) -> Vec<(i64, f64)> {
        if self.count == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.count);
        let mut cur_t = self.base_timestamp_ms;
        let mut cur_bits = self.base_value_bits;
        out.push((cur_t, f64::from_bits(cur_bits)));

        if self.count >= 2 {
            let mut cur_delta = self.first_delta_ms;
            cur_t += cur_delta;
            cur_bits ^= self.xor_residuals[0];
            out.push((cur_t, f64::from_bits(cur_bits)));

            for i in 0..self.delta_of_deltas_ms.len() {
                cur_delta += self.delta_of_deltas_ms[i];
                cur_t += cur_delta;
                cur_bits ^= self.xor_residuals[i + 1];
                out.push((cur_t, f64::from_bits(cur_bits)));
            }
        }
        out
    }

    /// Devuelve la fracción de lecturas periódicas cuyo Delta-of-Delta es `0` (tasa de compresión de 1 bit por marca de tiempo).
    pub fn zero_delta_of_delta_ratio(&self) -> f64 {
        if self.delta_of_deltas_ms.is_empty() {
            return 1.0;
        }
        let zeros = self
            .delta_of_deltas_ms
            .iter()
            .filter(|&&d2| d2 == 0)
            .count();
        zeros as f64 / self.delta_of_deltas_ms.len() as f64
    }

    /// Empaqueta `delta_of_deltas_ms` a nivel de bits:
    /// cada $\Delta^2_i == 0$ ocupa exactamente 1 bit (`0`); los valores no nulos ocupan el bit `1` seguido del delta firmado de 64 bits.
    pub fn pack_delta_of_deltas_bitstream(&self) -> Vec<u8> {
        let n = self.delta_of_deltas_ms.len();
        let bitmap_bytes = n.div_ceil(8);
        let mut out = vec![0u8; bitmap_bytes];
        let mut non_zero_payload = Vec::new();
        for (i, &d2) in self.delta_of_deltas_ms.iter().enumerate() {
            if d2 != 0 {
                out[i / 8] |= 1u8 << (i % 8);
                non_zero_payload.extend_from_slice(&d2.to_le_bytes());
            }
        }
        out.extend_from_slice(&non_zero_payload);
        out
    }

    fn unpack_delta_of_deltas_bitstream(bytes: &[u8], count: usize) -> DomainResult<Vec<i64>> {
        let bitmap_bytes = count.div_ceil(8);
        if bytes.len() < bitmap_bytes {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated Zone 3 Delta-of-Delta bitstream",
            ));
        }
        let bitmap = &bytes[0..bitmap_bytes];
        let mut cur = bitmap_bytes;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let is_non_zero = (bitmap[i / 8] & (1u8 << (i % 8))) != 0;
            if is_non_zero {
                if cur + 8 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Truncated non-zero Delta-of-Delta value in Zone 3",
                    ));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[cur..cur + 8]);
                cur += 8;
                out.push(i64::from_le_bytes(arr));
            } else {
                out.push(0);
            }
        }
        Ok(out)
    }

    /// Codifica la Zona 3 en un bloque binario alineado a 64 bytes con cabecera `"MBFZ3IOT"`, marcas de tiempo Delta-of-Delta, flujo Chimp128 + LZ4 y pirámides `1m`/`1h`/`1d`.
    pub fn encode_zone_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ZONE3_MAGIC);
        buf.extend_from_slice(&self.subject_row_id.to_le_bytes());
        buf.extend_from_slice(&self.metric_attr_id.to_le_bytes());
        let mc_bytes = self.metric_code.as_bytes();
        buf.extend_from_slice(&(mc_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(mc_bytes);

        buf.extend_from_slice(&(self.count as u32).to_le_bytes());
        buf.extend_from_slice(&self.base_timestamp_ms.to_le_bytes());
        buf.extend_from_slice(&self.first_delta_ms.to_le_bytes());
        buf.extend_from_slice(&self.base_value_bits.to_le_bytes());

        // 1. Bitpacked Delta-of-Delta Timestamps (1 bit per periodic reading)
        buf.extend_from_slice(&(self.delta_of_deltas_ms.len() as u32).to_le_bytes());
        let packed_dod = self.pack_delta_of_deltas_bitstream();
        buf.extend_from_slice(&(packed_dod.len() as u32).to_le_bytes());
        buf.extend_from_slice(&packed_dod);

        // 2. Chimp128 XOR Residuals compressed with LZ4 Frame
        let mut raw_xor_bytes = Vec::with_capacity(self.xor_residuals.len() * 8);
        for &xr in &self.xor_residuals {
            raw_xor_bytes.extend_from_slice(&xr.to_le_bytes());
        }
        let lz4_xor = lz4_flex::compress_prepend_size(&raw_xor_bytes);
        buf.extend_from_slice(&(self.xor_residuals.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(lz4_xor.len() as u32).to_le_bytes());
        buf.extend_from_slice(&lz4_xor);

        // 3. Multi-Resolution Pyramids (1m, 1h, 1d)
        Self::write_pyramid_buckets(&mut buf, &self.minute_pyramid);
        buf.extend_from_slice(&(self.hourly_pyramid.len() as u32).to_le_bytes());
        for &(hb, cnt, sum, min, max, last) in &self.hourly_pyramid {
            buf.extend_from_slice(&hb.to_le_bytes());
            buf.extend_from_slice(&cnt.to_le_bytes());
            buf.extend_from_slice(&sum.to_bits().to_le_bytes());
            buf.extend_from_slice(&min.to_bits().to_le_bytes());
            buf.extend_from_slice(&max.to_bits().to_le_bytes());
            buf.extend_from_slice(&last.to_bits().to_le_bytes());
        }
        Self::write_pyramid_buckets(&mut buf, &self.daily_pyramid);

        pad_to_64(&mut buf);
        buf
    }

    fn write_pyramid_buckets(buf: &mut Vec<u8>, buckets: &[PyramidRollupBucket]) {
        buf.extend_from_slice(&(buckets.len() as u32).to_le_bytes());
        for b in buckets {
            buf.extend_from_slice(&b.bucket_ms.to_le_bytes());
            buf.extend_from_slice(&b.count.to_le_bytes());
            buf.extend_from_slice(&b.sum_si.to_bits().to_le_bytes());
            buf.extend_from_slice(&b.min_si.to_bits().to_le_bytes());
            buf.extend_from_slice(&b.max_si.to_bits().to_le_bytes());
            buf.extend_from_slice(&b.first_si.to_bits().to_le_bytes());
            buf.extend_from_slice(&b.last_si.to_bits().to_le_bytes());
        }
    }

    fn read_pyramid_buckets(
        bytes: &[u8],
        cur: &mut usize,
    ) -> DomainResult<Vec<PyramidRollupBucket>> {
        let len = read_u32(bytes, cur)? as usize;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            let bucket_ms = read_i64(bytes, cur)?;
            let count = read_u64(bytes, cur)?;
            let sum_si = f64::from_bits(read_u64(bytes, cur)?);
            let min_si = f64::from_bits(read_u64(bytes, cur)?);
            let max_si = f64::from_bits(read_u64(bytes, cur)?);
            let first_si = f64::from_bits(read_u64(bytes, cur)?);
            let last_si = f64::from_bits(read_u64(bytes, cur)?);
            out.push(PyramidRollupBucket {
                bucket_ms,
                count,
                sum_si,
                min_si,
                max_si,
                first_si,
                last_si,
            });
        }
        Ok(out)
    }

    /// Decodifica un bloque `Chimp128SeriesBlock` de la Zona 3 a partir de su representación binaria alineada a 64 bytes.
    pub fn decode_zone_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 16 || bytes[0..8] != ZONE3_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid Zone 3 time-series binary magic header",
            ));
        }
        let mut cur = 8usize;
        let subject_row_id = read_u32(bytes, &mut cur)?;
        if cur + 4 > bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Unexpected EOF reading metric header in Zone 3",
            ));
        }
        let metric_attr_id = u16::from_le_bytes([bytes[cur], bytes[cur + 1]]);
        let mc_len = u16::from_le_bytes([bytes[cur + 2], bytes[cur + 3]]) as usize;
        cur += 4;
        if cur + mc_len > bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Unexpected EOF reading metric_code in Zone 3",
            ));
        }
        let metric_code = String::from_utf8_lossy(&bytes[cur..cur + mc_len]).into_owned();
        cur += mc_len;

        let count = read_u32(bytes, &mut cur)? as usize;
        let base_timestamp_ms = read_i64(bytes, &mut cur)?;
        let first_delta_ms = read_i64(bytes, &mut cur)?;
        let base_value_bits = read_u64(bytes, &mut cur)?;

        let dod_count = read_u32(bytes, &mut cur)? as usize;
        let dod_byte_len = read_u32(bytes, &mut cur)? as usize;
        if cur + dod_byte_len > bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Unexpected EOF in Zone 3 packed timestamps",
            ));
        }
        let delta_of_deltas_ms =
            Self::unpack_delta_of_deltas_bitstream(&bytes[cur..cur + dod_byte_len], dod_count)?;
        cur += dod_byte_len;

        let xor_count = read_u32(bytes, &mut cur)? as usize;
        let lz4_len = read_u32(bytes, &mut cur)? as usize;
        if cur + lz4_len > bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Unexpected EOF in Zone 3 LZ4 Chimp128 stream",
            ));
        }
        let decompressed_xor = lz4_flex::decompress_size_prepended(&bytes[cur..cur + lz4_len])
            .map_err(|e| {
                DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    format!("LZ4 decompression error in Zone 3: {e}"),
                )
            })?;
        cur += lz4_len;
        if decompressed_xor.len() < xor_count * 8 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Decompressed Chimp128 XOR buffer size mismatch",
            ));
        }
        let mut xor_residuals = Vec::with_capacity(xor_count);
        for i in 0..xor_count {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&decompressed_xor[i * 8..i * 8 + 8]);
            xor_residuals.push(u64::from_le_bytes(arr));
        }

        let minute_pyramid = Self::read_pyramid_buckets(bytes, &mut cur)?;
        let hr_len = read_u32(bytes, &mut cur)? as usize;
        let mut hourly_pyramid = Vec::with_capacity(hr_len);
        for _ in 0..hr_len {
            let hb = read_i64(bytes, &mut cur)?;
            let cnt = read_u64(bytes, &mut cur)?;
            let sum = f64::from_bits(read_u64(bytes, &mut cur)?);
            let min = f64::from_bits(read_u64(bytes, &mut cur)?);
            let max = f64::from_bits(read_u64(bytes, &mut cur)?);
            let last = f64::from_bits(read_u64(bytes, &mut cur)?);
            hourly_pyramid.push((hb, cnt, sum, min, max, last));
        }
        let daily_pyramid = Self::read_pyramid_buckets(bytes, &mut cur)?;

        Ok(Self {
            subject_row_id,
            metric_attr_id,
            metric_code,
            count,
            base_timestamp_ms,
            first_delta_ms,
            delta_of_deltas_ms,
            base_value_bits,
            xor_residuals,
            minute_pyramid,
            hourly_pyramid,
            daily_pyramid,
        })
    }
}

fn read_u32(bytes: &[u8], cur: &mut usize) -> DomainResult<u32> {
    if *cur + 4 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading u32 in Zone 3",
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

fn read_u64(bytes: &[u8], cur: &mut usize) -> DomainResult<u64> {
    if *cur + 8 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading u64 in Zone 3",
        ));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[*cur..*cur + 8]);
    *cur += 8;
    Ok(u64::from_le_bytes(arr))
}

fn read_i64(bytes: &[u8], cur: &mut usize) -> DomainResult<i64> {
    Ok(read_u64(bytes, cur)? as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chimp128_bitpacked_binary_roundtrip_and_affine_normalization() {
        let mut block = Chimp128SeriesBlock::new(10, "STREAM_SIGNAL_A");
        let f_to_c = AffineTransform::new(5.0 / 9.0, -32.0 * (5.0 / 9.0));
        let k_to_c = AffineTransform::new(1.0, -273.15);

        block.push_reading(1_700_000_000_000, 20.0, AffineTransform::IDENTITY);
        block.push_reading(1_700_000_060_000, 86.0, f_to_c);
        block.push_reading(1_700_000_120_000, 313.15, k_to_c);

        // Regular 60s interval means delta-of-delta is 0 -> bitpacked stream is 1 byte!
        assert!((block.zero_delta_of_delta_ratio() - 1.0).abs() < 1e-12);
        assert_eq!(block.pack_delta_of_deltas_bitstream().len(), 1);

        let bin = block.encode_zone_binary();
        assert_eq!(bin.len() % 64, 0);
        assert_eq!(&bin[0..8], b"MBFZ3IOT");

        let restored = Chimp128SeriesBlock::decode_zone_binary(&bin).unwrap();
        let points = restored.decompress_all();
        assert_eq!(points.len(), 3);
        assert!((points[0].1 - 20.0).abs() < 1e-11);
        assert!((points[1].1 - 30.0).abs() < 1e-11);
        assert!((points[2].1 - 40.0).abs() < 1e-11);
        assert_eq!(restored.minute_pyramid.len(), 3);
        assert_eq!(restored.hourly_pyramid.len(), 1);
        assert_eq!(restored.daily_pyramid.len(), 1);
    }
}
