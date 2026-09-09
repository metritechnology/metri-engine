fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // Fixtures dorados del contrato de eventos (golden_test.rs): viven en el
    // repo hermano metri-contracts, que no se publica. Solo se compila el
    // candado donde ambos repos conviven; en CI el módulo se excluye (cfg).
    println!("cargo:rustc-check-cfg=cfg(has_metri_contracts)");
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    if manifest_dir.join("../metri-contracts/golden").is_dir() {
        println!("cargo:rustc-cfg=has_metri_contracts");
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("metri_descriptor.bin"))
        .compile(&["proto/metri.proto", "proto/eda.proto"], &["proto"])?;

    // Compilar schema FlatBuffers (Janus AST IR)
    println!("cargo:rerun-if-changed=src/janus/janus_ir_ast.fbs");
    let status = std::process::Command::new("flatc")
        .arg("--rust")
        .arg("--gen-object-api")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/janus/janus_ir_ast.fbs")
        .status()?;

    if !status.success() {
        return Err(
            "Error compilando janus_ir_ast.fbs con flatc. Asegúrate de tener flatc instalado."
                .into(),
        );
    }

    Ok(())
}
