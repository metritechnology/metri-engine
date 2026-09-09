//! IoT telemetry — epoch bucketing and LZ4-compressed readings.
pub mod bucket;
pub use bucket::{compress_readings, decompress_readings, epoch_bucket, IoTReading};
