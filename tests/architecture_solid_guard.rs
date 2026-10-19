//! Guardián nativo de SOLID, DRY y Clean Code por archivo en `metri-engine`.
//!
//! Ejecutado automáticamente en `cargo test`, impone cuatro invariantes duras:
//! 1. **Clean Code (`unsafe` = 0):** ningún archivo productivo de `src/` puede
//!    contener bloques ni funciones `unsafe`.
//! 2. **ISP (Segregación de Interfaces):** ningún trait de puerto en
//!    `src/domain/protocols.rs` ni `src/application/ports.rs` puede superar los
//!    5 métodos cohesivos.
//! 3. **DIP (Frontera Hexagonal en `codice`, `temporal`, `application` y `iop/cedar_step`):**
//!    los módulos ya saneados del núcleo mantienen `0` dependencias hacia
//!    `aws_sdk_*`, `tonic`, `lambda_runtime`, `crate::grpc` o `crate::infrastructure`.
//! 4. **Ratchet Integral por Archivo (`check_clean_code.py`):** ningún archivo
//!    de `metri-engine` (Rust o Python) puede introducir una violación nueva de
//!    SRP, DIP, ISP, DRY o Clean Code sobre `scripts/dev/clean_code_baseline.json`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const MAX_PORT_TRAIT_METHODS: usize = 5;

fn manifest_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn is_test_or_generated_rs(rel_from_src: &Path) -> bool {
    if rel_from_src
        .components()
        .any(|c| matches!(c.as_os_str().to_str(), Some("tests" | "testing")))
    {
        return true;
    }
    let Some(name) = rel_from_src.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    name == "test_support.rs"
        || name == "fbs.rs"
        || name.starts_with("test_")
        || name.ends_with("_tests.rs")
        || name.ends_with("_test.rs")
}

fn prod_rust_files(src_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![src_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                if let Ok(rel) = path.strip_prefix(src_dir) {
                    if !is_test_or_generated_rs(rel) {
                        out.push(path);
                    }
                }
            }
        }
    }
    out.sort();
    out
}

/// Corta el contenido de un archivo `.rs` justo antes de `#[cfg(test)]`.
fn prod_region(content: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        if line.trim_start().starts_with("#[cfg(test)]") {
            break;
        }
        out.push((idx + 1, line));
    }
    out
}

#[test]
fn clean_code_cero_bloques_unsafe_en_produccion() {
    let src = manifest_root().join("src");
    let mut hits = Vec::new();
    for file in prod_rust_files(&src) {
        let content = fs::read_to_string(&file).unwrap_or_default();
        for (line_no, line) in prod_region(&content) {
            let code = line.split("//").next().unwrap_or("").trim();
            if code.starts_with("unsafe ")
                || code.contains(" unsafe ")
                || code.starts_with("unsafe{")
                || code.contains(" unsafe{")
            {
                hits.push(format!("{}:{line_no} -> {code}", file.display()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "Violación Clean Code: bloques `unsafe` prohibidos en src/:\n  - {}",
        hits.join("\n  - ")
    );
}

#[test]
fn isp_puertos_de_dominio_y_aplicacion_esbeltos() {
    let root = manifest_root();
    for rel in ["src/domain/protocols.rs", "src/application/ports.rs"] {
        let path = root.join(rel);
        let content = fs::read_to_string(&path).expect("archivo de puertos debe existir");
        let lines = prod_region(&content);
        let mut idx = 0;
        while idx < lines.len() {
            let trimmed = lines[idx].1.trim_start();
            if let Some(rest) = trimmed.strip_prefix("pub trait ") {
                let trait_name = rest
                    .split(|c: char| !c.is_alphanumeric() && c != '_')
                    .next()
                    .unwrap_or("unknown");
                let mut depth: i32 = 0;
                let mut started = false;
                let mut method_count = 0usize;
                let mut k = idx;
                while k < lines.len() {
                    let code = lines[k].1.split("//").next().unwrap_or("").trim();
                    if started
                        && depth == 1
                        && (code.starts_with("fn ") || code.starts_with("async fn "))
                    {
                        method_count += 1;
                    }
                    depth += code.matches('{').count() as i32;
                    depth -= code.matches('}').count() as i32;
                    if code.contains('{') {
                        started = true;
                    }
                    if started && depth <= 0 {
                        break;
                    }
                    k += 1;
                }
                assert!(
                    method_count <= MAX_PORT_TRAIT_METHODS,
                    "Violación ISP en {rel}: trait `{trait_name}` tiene {method_count} métodos (máx {MAX_PORT_TRAIT_METHODS})"
                );
                idx = k + 1;
                continue;
            }
            idx += 1;
        }
    }
}

#[test]
fn dip_modulos_saneados_sin_acoplamiento_a_infraestructura_o_grpc() {
    let root = manifest_root();
    let clean_core_files = [
        "src/codice/generator.rs",
        "src/codice/sequence.rs",
        "src/codice/registry.rs",
        "src/codice/validator.rs",
        "src/iop/cedar_step.rs",
        "src/iop/janus_step.rs",
        "src/iop/quota_step.rs",
        "src/application/ports.rs",
    ];
    let forbidden = [
        "aws_sdk_",
        "tonic::",
        "lambda_runtime",
        "crate::grpc",
        "crate::infrastructure",
    ];

    let mut violations = Vec::new();
    for rel in clean_core_files {
        let content = fs::read_to_string(root.join(rel)).unwrap_or_default();
        for (line_no, line) in prod_region(&content) {
            let code = line.split("//").next().unwrap_or("");
            for bad in forbidden {
                if code.contains(bad) {
                    violations.push(format!("{rel}:{line_no} importa `{bad}`"));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "Violación DIP (Arquitectura Hexagonal) en módulos saneados:\n  - {}",
        violations.join("\n  - ")
    );
}

#[test]
fn ratchet_por_archivo_solid_dry_y_clean_code_en_verde() {
    let root = manifest_root();
    let script = root.join("scripts/dev/check_clean_code.py");
    let output = Command::new("python3")
        .arg(&script)
        .current_dir(&root)
        .output()
        .expect("debe poder ejecutar python3 scripts/dev/check_clean_code.py");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "El gate por archivo SOLID/DRY/Clean Code falló:\n{stdout}\n{stderr}"
    );
}
