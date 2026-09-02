// aegis/sql/security.rs
// SRP: Encapsular el gate de seguridad Zero Trust (ZT Invariant §7) para verificar aislamiento de inquilinos.

use serde_json::Value;

/// Verifica que el AST IR contiene `tenant_id` en el árbol `:where`.
/// Gate de seguridad §7 — si retorna false, el SQL NO se genera nunca.
/// [PORTED_FROM: (ast-contains-tenant? ast-ir)]
pub fn ast_contains_tenant(ast_ir: &Value) -> bool {
    fn scan(node: &Value) -> bool {
        if let Some(arr) = node.as_array() {
            if arr.is_empty() {
                return false;
            }
            if let Some(op_str) = arr[0].as_str() {
                match op_str {
                    "=" => {
                        if let Some(field_str) = arr.get(1).and_then(|v| v.as_str()) {
                            return field_str == "entity/tenant-id"
                                || field_str == "tenant/id"
                                || field_str.contains("tenant");
                        }
                    }
                    "and" | "or" => {
                        for child in arr.iter().skip(1) {
                            if scan(child) {
                                return true;
                            }
                        }
                    }
                    "not" => {
                        if let Some(child) = arr.get(1) {
                            return scan(child);
                        }
                    }
                    "ref-filter" => return false, // Apunta a otra entidad
                    _ => {}
                }
            }
        }
        false
    }

    if let Some(where_node) = ast_ir.get("where") {
        scan(where_node)
    } else {
        false
    }
}
