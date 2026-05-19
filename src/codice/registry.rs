// [PORTED_FROM: src/metri/codice/registry.clj + src/metri/codice/api.clj]
// Equivalencia: CodeRegistry — carga, valida, hashea y compila todos los modelos JSON.
// En Clojure: Integrant ig/init-key :codice/registry + atom global
// En Rust:    OnceLock<CodeRegistry> — inmutable post-bootstrap, lookup O(1)
//
// Zero-Drop Policy: todos los guard de error (COD_001, COD_002, COD_003, COD_SCOPE_001)
// están replicados con exactamente la misma semántica.

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use crate::domain::errors::{DomainError, ErrorCode};

// ── Tipos de valor de los atributos ──────────────────────────────────────────

/// Mapea 1:1 con los tipos JSON del Códice:
/// "string" | "number" | "epoch" | "decimal" | "boolean" | "array" | "reference"
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttrType {
    String,
    Number,
    Epoch,
    Decimal,
    Boolean,
    Array,
    Reference,
    Uuid,
    Bytes,
    Enum, // <--- added
    Unknown(String),
}

impl From<&str> for AttrType {
    fn from(s: &str) -> Self {
        match s {
            "string"    => AttrType::String,
            "number"    => AttrType::Number,
            "epoch"     => AttrType::Epoch,
            "decimal"   => AttrType::Decimal,
            "boolean"   => AttrType::Boolean,
            "array"     => AttrType::Array,
            "reference" => AttrType::Reference,
            "uuid"      => AttrType::Uuid,
            "bytes"     => AttrType::Bytes,
            "enum"      => AttrType::Enum,
            "double"    => AttrType::Decimal,
            "float"     => AttrType::Decimal,
            other       => AttrType::Unknown(other.to_string()),
        }
    }
}

/// Canal de ejecución del engine.
/// Equivale a (keyword (get model :engine "oltp")) en Clojure.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineChannel {
    Oltp,
    Olap,
}

impl From<&str> for EngineChannel {
    fn from(s: &str) -> Self {
        match s {
            "olap" => EngineChannel::Olap,
            _      => EngineChannel::Oltp, // default seguro
        }
    }
}

/// Descriptor de un atributo del modelo JSON.
/// Equivale al mapa de atributos en el JSON del Códice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDescriptor {
    pub name:                    String,
    pub attr_type:               AttrType,
    pub label:                   Option<String>,
    pub required:                bool,
    pub unique:                  Option<String>, // "identity" | "value" | null
    pub indexed:                 bool,           // genera item AVET
    pub fts:                     bool,           // Full-text search index
    pub is_dimension:            bool,           // dimension para OLAP
    pub is_metric:               bool,           // métrica para OLAP
    pub entity_ref:              Option<String>, // referencia a otra entidad
    pub options:                 Vec<String>,    // enum values
    pub is_sequence_scope:       bool,
    pub is_sequence_scope_via:   bool,
    /// Marca PII — sanitizado en error_response (Clojure: :sensitive true).
    /// [PORTED_FROM: (filter :sensitive (:attributes schema))]
    pub sensitive:               bool,
}

/// Modelo completo de una entidad del Códice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityModel {
    pub entity:      String,
    pub label:       Option<String>,
    pub icon:        Option<String>,
    pub primary_key: Option<String>,
    pub fts_fields:  Vec<String>,
    pub engine:      EngineChannel,
    pub attributes:  Vec<AttributeDescriptor>,
    pub event_rules: Vec<serde_json::Value>,
    pub is_sequence_scope_provider: bool,
}

/// Entrada del registry para una entidad.
#[derive(Debug, Clone)]
pub struct RegistryEntry {
    pub model:       EntityModel,
    pub fingerprint: String, // SHA-256 hex — COD_003 collision detection
}

/// CodeRegistry — registro en memoria de todos los modelos compilados.
/// Lookup O(1) por entity_name.
/// [PORTED_FROM: (def ^:private registry (atom {})) + ig/init-key :codice/registry]
pub struct CodeRegistry {
    /// Lookup por nombre de entidad → entry
    by_entity:   HashMap<String, RegistryEntry>,
    /// Lookup por fingerprint → entity_name (para COD_003)
    by_hash:     HashMap<String, String>,
}

impl CodeRegistry {
    /// Construye el registry escaneando todos los JSON en `models_dir`.
    /// Lanza DomainError en cualquier condición de error (fail-fast de bootstrap).
    /// [PORTED_FROM: (build-registry models-dir)]
    pub fn build(models_dir: &Path) -> Result<(Self, Vec<serde_json::Value>), DomainError> {
        let files = collect_json_files(models_dir)?;

        if files.is_empty() {
            return Err(DomainError::codice(
                ErrorCode::Cod002,
                format!("Códice: no JSON model files found in: {}", models_dir.display()),
            ));
        }

        info!("Códice: scanning {} JSON models from {}", files.len(), models_dir.display());

        let mut by_entity:        HashMap<String, RegistryEntry> = HashMap::new();
        let mut by_hash:          HashMap<String, String>        = HashMap::new();
        let mut event_rules_seed: Vec<serde_json::Value>         = Vec::new();

        for file in &files {
            let raw = std::fs::read_to_string(file).map_err(|e| {
                DomainError::codice(ErrorCode::Cod001, format!("Cannot read {:?}: {e}", file))
            })?;
            let json: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
                DomainError::codice(ErrorCode::Cod001, format!("Cannot parse {:?}: {e}", file))
            })?;

            let entity_name = json["entity"]
                .as_str()
                .ok_or_else(|| DomainError::codice(ErrorCode::Cod001, "Missing 'entity' field"))?
                .to_string();

            // Guard COD_002: entidad duplicada
            // [PORTED_FROM: (when (contains? registry entity) (throw ...))]
            if by_entity.contains_key(&entity_name) {
                return Err(DomainError::codice(
                    ErrorCode::Cod002,
                    format!("COD_002: Duplicate entity in Códice: '{entity_name}'"),
                ));
            }

            let fingerprint = schema_fingerprint(&entity_name, &json);

            // Guard COD_003: colisión de fingerprint
            // [PORTED_FROM: (when-let [existing (get hashes hash)] (throw ...))]
            if let Some(existing) = by_hash.get(&fingerprint) {
                return Err(DomainError::codice(
                    ErrorCode::Cod003,
                    format!(
                        "COD_003: SHA-256 collision: '{entity_name}' y '{existing}' \
                         tienen el mismo fingerprint"
                    ),
                ));
            }

            let model = parse_entity_model(&json)?;

            // Acumular event_rules para seed posterior
            // [PORTED_FROM: (extract-event-rules model)]
            if let Some(rules) = json["event_rules"].as_array() {
                for rule in rules {
                    let mut rule = rule.clone();
                    if let Some(obj) = rule.as_object_mut() {
                        obj.insert(
                            "target_entity_name".to_string(),
                            serde_json::Value::String(entity_name.clone()),
                        );
                        obj.insert(
                            "is_system_seeded".to_string(),
                            serde_json::Value::Bool(true),
                        );
                    }
                    event_rules_seed.push(rule);
                }
            }

            by_hash.insert(fingerprint.clone(), entity_name.clone());
            by_entity.insert(entity_name, RegistryEntry { model, fingerprint });
        }

        let registry = CodeRegistry { by_entity, by_hash };

        // Post-build: validar scope providers
        // [PORTED_FROM: (validate-scope-providers! registry)]
        registry.validate_scope_providers()?;

        info!(
            "Códice: registry built — {} entities, {} event rules",
            registry.by_entity.len(),
            event_rules_seed.len()
        );

        Ok((registry, event_rules_seed))
    }

    // ── API pública — lookup O(1) ────────────────────────────────────────────

    /// Obtiene el modelo de una entidad.
    /// [PORTED_FROM: (entity-model entity-type ctx)]
    pub fn get_model(&self, entity_type: &str) -> Option<&EntityModel> {
        self.by_entity.get(entity_type).map(|e| &e.model)
    }

    /// Obtiene el engine channel de una entidad.
    /// [PORTED_FROM: (entity-engine entity-type ctx)]
    pub fn get_engine(&self, entity_type: &str) -> Option<&EngineChannel> {
        self.by_entity.get(entity_type).map(|e| &e.model.engine)
    }

    /// Obtiene los atributos de una entidad.
    /// [PORTED_FROM: (describe-attributes entity-type ctx)]
    pub fn get_attributes(&self, entity_type: &str) -> Option<&[AttributeDescriptor]> {
        self.by_entity
            .get(entity_type)
            .map(|e| e.model.attributes.as_slice())
    }

    /// Obtiene un atributo específico por nombre.
    pub fn get_attribute(&self, entity_type: &str, attr_name: &str) -> Option<&AttributeDescriptor> {
        self.get_attributes(entity_type)
            .and_then(|attrs| attrs.iter().find(|a| a.name == attr_name))
    }

    /// Obtiene el fingerprint SHA-256.
    /// [PORTED_FROM: (entity-hash entity-type)]
    pub fn get_fingerprint(&self, entity_type: &str) -> Option<&str> {
        self.by_entity.get(entity_type).map(|e| e.fingerprint.as_str())
    }

    /// Lista todos los nombres de entidades registradas.
    pub fn entity_names(&self) -> impl Iterator<Item = &str> {
        self.by_entity.keys().map(|s| s.as_str())
    }

    /// Total de entidades registradas.
    pub fn entity_count(&self) -> usize {
        self.by_entity.len()
    }

    // ── Validación post-build ────────────────────────────────────────────────

    /// Valida que los atributos con is_sequence_scope apunten a un
    /// entityRef que sea is_sequence_scope_provider: true.
    /// [PORTED_FROM: (validate-scope-providers! registry)]
    fn validate_scope_providers(&self) -> Result<(), DomainError> {
        for (entity_name, entry) in &self.by_entity {
            for attr in &entry.model.attributes {
                if attr.is_sequence_scope || attr.is_sequence_scope_via {
                    if let Some(entity_ref) = &attr.entity_ref {
                        if let Some(provider_entry) = self.by_entity.get(entity_ref) {
                            let is_provider = provider_entry.model.is_sequence_scope_provider;
                            if !is_provider {
                                return Err(DomainError::codice(
                                    ErrorCode::CodScope001,
                                    format!(
                                        "COD_SCOPE_001: entity '{entity_name}' attr '{}' \
                                         → entityRef '{entity_ref}' is not is_sequence_scope_provider:true",
                                        attr.name
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// ── Registro estático global — equivale al `(atom {})` de Clojure ────────────
/// OnceLock garantiza que se inicialice UNA sola vez en el cold start.
/// [PORTED_FROM: (def ^:private registry (atom {}))]
static REGISTRY: OnceLock<CodeRegistry> = OnceLock::new();

/// Inicializa el registry global. Llamado UNA sola vez en main.rs.
/// [PORTED_FROM: (api/init! registry)]
pub fn init_global(registry: CodeRegistry) {
    REGISTRY.set(registry).unwrap_or_else(|_| {
        panic!("CodeRegistry ya fue inicializado — no llamar init_global dos veces")
    });
    info!("Códice: registry global activo");
}

/// Accede al registry global. Panics si no fue inicializado.
pub fn global() -> &'static CodeRegistry {
    REGISTRY.get().expect("CodeRegistry no inicializado — llamar init_global primero")
}

// ── Helpers privados ─────────────────────────────────────────────────────────

/// Genera fingerprint SHA-256 del modelo.
/// [PORTED_FROM: (schema-fingerprint [{:keys [entity attributes]}])]
fn schema_fingerprint(entity_name: &str, json: &serde_json::Value) -> String {
    let attr_names: Vec<&str> = json["attributes"]
        .as_array()
        .map(|attrs| {
            attrs
                .iter()
                .filter_map(|a| a["name"].as_str())
                .collect()
        })
        .unwrap_or_default();

    let input = format!("{entity_name}{attr_names:?}");
    let hash = Sha256::digest(input.as_bytes());
    hex::encode(hash)
}

/// Parsea un modelo JSON en un EntityModel tipado.
/// [PORTED_FROM: (load-model-file file)]
fn parse_entity_model(json: &serde_json::Value) -> Result<EntityModel, DomainError> {
    let entity = json["entity"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    let engine = EngineChannel::from(json["engine"].as_str().unwrap_or("oltp"));

    let attributes = json["attributes"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|a| AttributeDescriptor {
            name:                    a["name"].as_str().unwrap_or("").to_string(),
            attr_type:               AttrType::from(a["type"].as_str().unwrap_or("string")),
            label:                   a["label"].as_str().map(str::to_string),
            required:                a["required"].as_bool().unwrap_or(false),
            unique:                  a["unique"].as_str().map(str::to_string),
            indexed:                 a["index"].as_bool().unwrap_or(false),
            fts:                     a["fts"].as_bool().unwrap_or(false),
            is_dimension:            a["is_dimension"].as_bool().unwrap_or(false),
            is_metric:               a["is_metric"].as_bool().unwrap_or(false),
            entity_ref:              a["entityRef"].as_str().map(str::to_string),
            options:                 a["options"]
                                        .as_array()
                                        .map(|o| {
                                            o.iter()
                                             .filter_map(|v| v.as_str().map(str::to_string))
                                             .collect()
                                        })
                                        .unwrap_or_default(),
            is_sequence_scope:       a["is_sequence_scope"].as_bool().unwrap_or(false),
            is_sequence_scope_via:   a["is_sequence_scope_via"].as_bool().unwrap_or(false),
            // [PORTED_FROM: (filter :sensitive (:attributes schema))]
            sensitive:               a["sensitive"].as_bool().unwrap_or(false),
        })
        .collect();

    let fts_fields = json["fts_fields"]
        .as_array()
        .map(|f| f.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();

    let event_rules = json["event_rules"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    Ok(EntityModel {
        entity,
        label:       json["label"].as_str().map(str::to_string),
        icon:        json["icon"].as_str().map(str::to_string),
        primary_key: json["primary_key"].as_str().map(str::to_string),
        fts_fields,
        engine,
        attributes,
        event_rules,
        is_sequence_scope_provider: json["is_sequence_scope_provider"].as_bool().unwrap_or(false),
    })
}

/// Recolecta todos los archivos .json de un directorio.
/// [PORTED_FROM: (list-model-files models-dir)]
fn collect_json_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, DomainError> {
    if !dir.is_dir() {
        return Err(DomainError::codice(
            ErrorCode::Cod001,
            format!("Models directory does not exist: {}", dir.display()),
        ));
    }

    let mut files: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| DomainError::codice(ErrorCode::Cod001, format!("Cannot read dir: {e}")))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()?.to_str()? == "json" {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    // Ordenar por nombre de archivo para determinismo
    files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    Ok(files)
}
