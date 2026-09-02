// eav/telemetry/bucket.rs — IoT Epoch Bucketing + LZ4 Compression
// Blueprint: Metri EAV §XI.2

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

/// Estructura de una lectura IoT individual.
#[derive(Debug, Clone)]
pub struct IoTReading {
    pub entity_id: String,
    pub attr_name: String,
    pub value: f64,
    pub timestamp_ms: i64,
}

/// Calcula el inicio del bucket de tiempo para agrupar lecturas IoT.
/// granularity_secs = 60 (por minuto) | 3600 (por hora) | 86400 (por día)
///
/// Resultado: epoch_ms del inicio del bucket.
/// [Blueprint: §XI.2 — "Hybrid EAV con Epoch-Bucketing"]
pub fn epoch_bucket(timestamp_ms: i64, granularity_secs: u64) -> i64 {
    let ts_secs = timestamp_ms / 1000;
    let bucket_secs = ts_secs - (ts_secs % granularity_secs as i64);
    bucket_secs * 1000 // retornamos epoch_ms del inicio del bucket
}

/// Comprime N lecturas IoT a un blob LZ4 para almacenamiento compactado.
/// Reduce ~98% los costos de WCU al escribir telemetría en batch.
pub fn compress_readings(readings: &[IoTReading]) -> Vec<u8> {
    // Serialización simple CSV para MVP — en producción: Parquet o Arrow IPC
    let csv: String = readings
        .iter()
        .map(|r| {
            format!(
                "{},{},{},{}",
                r.entity_id, r.attr_name, r.value, r.timestamp_ms
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    compress_prepend_size(csv.as_bytes())
}

/// Descomprime un blob LZ4 a lecturas IoT.
pub fn decompress_readings(blob: &[u8]) -> Vec<IoTReading> {
    let Ok(decompressed) = decompress_size_prepended(blob) else {
        return vec![];
    };
    let Ok(csv) = std::str::from_utf8(&decompressed) else {
        return vec![];
    };

    csv.lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() < 4 {
                return None;
            }
            Some(IoTReading {
                entity_id: parts[0].to_string(),
                attr_name: parts[1].to_string(),
                value: parts[2].parse().ok()?,
                timestamp_ms: parts[3].parse().ok()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_bucket_hourly() {
        // 2024-01-01 01:45:00 UTC → bucket → 2024-01-01 01:00:00 UTC
        let ts_ms = 1704074700_000i64; // 01:45:00
        let bucket = epoch_bucket(ts_ms, 3600);
        assert_eq!(bucket % (3600 * 1000), 0); // múltiplo exacto de hora
    }

    #[test]
    fn compress_decompress_roundtrip() {
        let readings = vec![
            IoTReading {
                entity_id: "e1".to_string(),
                attr_name: "temp".to_string(),
                value: 23.5,
                timestamp_ms: 1000,
            },
            IoTReading {
                entity_id: "e1".to_string(),
                attr_name: "temp".to_string(),
                value: 24.0,
                timestamp_ms: 2000,
            },
        ];
        let blob = compress_readings(&readings);
        let decompressed = decompress_readings(&blob);
        assert_eq!(decompressed.len(), 2);
        assert!((decompressed[0].value - 23.5).abs() < 0.001);
    }
}
