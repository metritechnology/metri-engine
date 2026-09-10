//! CodeRegistry — loads, validates, hashes and compiles every JSON model.
//!
//! # Origin
//! Equivalencia: CodeRegistry — carga, valida, hashea y compila todos los modelos JSON.
//! En el stack anterior: Integrant ig/init-key :codice/registry + atom global
//! En Rust:    OnceLock<CodeRegistry> — inmutable post-bootstrap, lookup O(1)
//!
//! Zero-Drop Policy: todos los guard de error (COD_001, COD_002, COD_003, COD_SCOPE_001)
//! están replicados con exactamente la misma semántica.

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::info;

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
    Json,
    Unknown(String),
}

impl From<&str> for AttrType {
    fn from(s: &str) -> Self {
        match s {
            "string" => AttrType::String,
            "number" => AttrType::Number,
            "integer" => AttrType::Number,
            "int" => AttrType::Number,
            "long" => AttrType::Number,
            "epoch" => AttrType::Epoch,
            "decimal" => AttrType::Decimal,
            "boolean" => AttrType::Boolean,
            "array" => AttrType::Array,
            "reference" => AttrType::Reference,
            "uuid" => AttrType::Uuid,
            "bytes" => AttrType::Bytes,
            "enum" => AttrType::Enum,
            "json" => AttrType::Json,
            "double" => AttrType::Decimal,
            "float" => AttrType::Decimal,
            other => AttrType::Unknown(other.to_string()),
        }
    }
}

/// Canal de ejecución del engine.
/// Equivale a (keyword (get model :engine "oltp")) en el stack anterior.
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
            _ => EngineChannel::Oltp, // default seguro
        }
    }
}

/// Descriptor de un atributo del modelo JSON.
/// Equivale al mapa de atributos en el JSON del Códice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDescriptor {
    pub name: String,
    pub attr_type: AttrType,
    pub label: Option<String>,
    pub required: bool,
    pub unique: Option<String>, // "tenant" | "identity" | "value" | null
    pub indexed: bool,          // genera item AVET
    pub fts: bool,              // Full-text search index
    pub is_dimension: bool,     // dimension para OLAP
    pub is_metric: bool,        // métrica para OLAP
    pub entity_ref: Option<String>, // referencia a otra entidad
    pub options: Vec<String>,   // enum values
    pub is_sequence_scope: bool,
    pub is_sequence_scope_via: bool,
    /// /// Marca PII — sanitizado en error_response (el stack anterior: :sensitive true).
    pub sensitive: bool,
    pub auto_generate: Option<serde_json::Value>,
    pub validation_regex: Option<String>,
    pub default_value: Option<String>,
}

/// Ámbito de una restricción de unicidad.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintScope {
    /// Único dentro del tenant. Es el caso normal.
    Tenant,
    /// Único en toda la plataforma (identidades globales).
    Global,
}

impl From<&str> for ConstraintScope {
    fn from(s: &str) -> Self {
        match s {
            "global" | "identity" => ConstraintScope::Global,
            _ => ConstraintScope::Tenant,
        }
    }
}

/// Restricción declarada a nivel de entidad.
///
/// Existe porque `unique` es por atributo y no puede expresar
/// `(tenant_id, plugin_id)`. Esa carencia es la razón física de que dos
/// entidades `tenant_plugin` pudieran coexistir para el mismo módulo, que fue
/// el disparador del incidente de `cmms`.
///
/// Se declara así:
///
/// ```json
/// "constraints": [
///   { "type": "unique", "scope": "tenant", "attributes": ["tenant_id", "plugin_id"] }
/// ]
/// ```
///
/// Tipos adicionales (evaluados sobre la vista fusionada estado-previo +
/// payload en el camino de escritura):
///
/// ```json
/// "constraints": [
///   { "type": "requires_when", "when": {"status": "COMPLETED"},
///     "required": ["completed_by", "completed_at"] },
///   { "type": "at_most", "field": "score", "of": "max_score" },
///   { "type": "ref_state", "attr": "procedure_id",
///     "conditions": {"lifecycle_state": "PUBLISHED"} }
/// ]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    pub kind: ConstraintKind,
    pub scope: ConstraintScope,
    /// Atributos de la restricción, EN EL ORDEN DECLARADO.
    ///
    /// `unique`: la clave física (reordenar cambia el hash y deja huérfanos
    /// los items ya escritos). `requires_when`: los campos exigidos.
    /// `at_most`: `[field, of]`. `ref_state`: `[attr]` — el atributo
    /// referencia cuya entidad apuntada debe cumplir `when`.
    pub attributes: Vec<String>,
    /// Pares (campo, valor) que condicionan `RequiresWhen` y `RefState`.
    #[serde(default)]
    pub when: Option<Vec<(String, String)>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintKind {
    Unique,
    /// Si todos los pares `when` se cumplen en la vista fusionada, los campos
    /// `attributes` son obligatorios.
    RequiresWhen,
    /// `attributes[0]` no puede exceder a `attributes[1]` (numérico).
    AtMost,
    /// `attributes[0]` no puede ser menor a `attributes[1]` (numérico).
    /// Espejo de `AtMost` — p. ej. end_time ≥ start_time.
    AtLeast,
    /// Al menos UNO de los campos en `attributes` debe estar presente.
    RequiresAny,
    /// La entidad apuntada por `attributes[0]` debe cumplir todos los pares
    /// `when`. Comprobación con lectura: tolera la carrera por diseño (ver
    /// `constraints::check_ref_conditions`).
    RefState,
}

/// Modelo completo de una entidad del Códice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityModel {
    pub entity: String,
    pub label: Option<String>,
    pub icon: Option<String>,
    pub primary_key: Option<String>,
    pub fts_fields: Vec<String>,
    pub engine: EngineChannel,
    pub attributes: Vec<AttributeDescriptor>,
    pub event_rules: Vec<serde_json::Value>,
    pub is_sequence_scope_provider: bool,
    #[serde(default)]
    pub write_path_locked: bool,
    #[serde(default)]
    pub is_system: bool,
    #[serde(default)]
    pub disable_eda: bool,
    /// Mapa de proyección declarativa hacia `scheduled_job`.
    /// Clave: ruta destino en el scheduled_job (los puntos descienden al action_payload).
    /// Valor: ruta origen — `[campo]` lee del payload padre;
    ///        `[campo_ref, attr]` sigue la referencia y lee `attr` de la entidad referenciada.
    #[serde(default)]
    pub shadow_sagas_mapping: Option<serde_json::Value>,
    /// Restricciones a nivel de entidad, declaradas en el JSON del modelo.
    ///
    /// `work_order` declara la primera (unique tenant de `scheduled_job_id` +
    /// `asset_id`, idempotencia del loop preventivo). Activar una restricción
    /// con duplicados vivos en la base haría fallar la siguiente escritura de
    /// esos tenants: cada nueva constraint exige conciliación previa del dato.
    #[serde(default)]
    pub constraints: Vec<Constraint>,
}

/// Entrada del registry para una entidad.
#[derive(Debug, Clone)]
pub struct RegistryEntry {
    pub model: EntityModel,
    pub fingerprint: String, // SHA-256 hex — COD_003 collision detection
}

/// CodeRegistry — registro en memoria de todos los modelos compilados.
/// Lookup O(1) por entity_name.
pub struct CodeRegistry {
    /// Lookup por nombre de entidad → entry
    by_entity: HashMap<String, RegistryEntry>,
    /// Lookup inverso: DJB2 hash de atributo → nombre de atributo
    by_attr_id: HashMap<u16, String>,
    /// Diccionarios de localización: locale -> JSON
    locales: HashMap<String, serde_json::Value>,
}

impl CodeRegistry {
    /// Construye el registry escaneando todos los JSON en `models_dir`.
    /// Lanza DomainError en cualquier condición de error (fail-fast de bootstrap).
    pub fn build(models_dir: &Path) -> Result<(Self, Vec<serde_json::Value>), DomainError> {
        let files = collect_json_files(models_dir)?;

        if files.is_empty() {
            return Err(DomainError::codice(
                ErrorCode::Cod002,
                format!(
                    "Códice: no JSON model files found in: {}",
                    models_dir.display()
                ),
            ));
        }

        info!(
            "Códice: scanning {} JSON models from {}",
            files.len(),
            models_dir.display()
        );

        let mut by_entity: HashMap<String, RegistryEntry> = HashMap::new();
        let mut by_hash: HashMap<String, String> = HashMap::new();
        let mut by_attr_id: HashMap<u16, String> = HashMap::new();
        let mut event_rules_seed: Vec<serde_json::Value> = Vec::new();

        // Cargar archivos de diccionarios localizados recursivamente
        let mut locales = HashMap::new();
        let locales_dir = models_dir
            .parent()
            .map(|p| p.join("locales"))
            .unwrap_or_else(|| Path::new("config/locales").to_path_buf());
        if locales_dir.is_dir() {
            fn merge_json(a: &mut serde_json::Value, b: serde_json::Value) {
                match (a, b) {
                    (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
                        for (k, v) in b {
                            merge_json(a.entry(k).or_insert(serde_json::Value::Null), v);
                        }
                    }
                    (a, b) => *a = b,
                }
            }

            fn load_locales_recursive(
                dir: &Path,
                base_dir: &Path,
                locales: &mut HashMap<String, serde_json::Value>,
            ) {
                if let Ok(entries) = std::fs::read_dir(dir) {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            let path = entry.path();
                            if path.is_dir() {
                                load_locales_recursive(&path, base_dir, locales);
                            } else if path.extension().and_then(|s| s.to_str()) == Some("json") {
                                if let Ok(rel) = path.strip_prefix(base_dir) {
                                    if let Some(first_comp) = rel.components().next() {
                                        if let Some(locale_name) = first_comp.as_os_str().to_str() {
                                            let clean_locale = if locale_name.ends_with(".json") {
                                                locale_name.trim_end_matches(".json")
                                            } else {
                                                locale_name
                                            };
                                            if let Ok(content) = std::fs::read_to_string(&path) {
                                                if let Ok(json) =
                                                    serde_json::from_str::<serde_json::Value>(
                                                        &content,
                                                    )
                                                {
                                                    let locale_entry = locales
                                                        .entry(clean_locale.to_string())
                                                        .or_insert_with(|| {
                                                            serde_json::Value::Object(
                                                                serde_json::Map::new(),
                                                            )
                                                        });
                                                    merge_json(locale_entry, json);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            load_locales_recursive(&locales_dir, &locales_dir, &mut locales);
        }

        // Registrar atributos de sistema reservados
        by_attr_id.insert(0x0000, "entity/ulid".to_string());
        by_attr_id.insert(0x0001, "entity/type".to_string());
        by_attr_id.insert(0x0002, "tenant/id".to_string());
        by_attr_id.insert(0x0003, "meta/created_at".to_string());
        by_attr_id.insert(0x0004, "meta/updated_at".to_string());
        by_attr_id.insert(
            crate::eav::types::datom::Datom::hash_attr_name("entity_type"),
            "entity_type".to_string(),
        );

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
            if by_entity.contains_key(&entity_name) {
                return Err(DomainError::codice(
                    ErrorCode::Cod002,
                    format!("COD_002: Duplicate entity in Códice: '{entity_name}'"),
                ));
            }

            let fingerprint = schema_fingerprint(&entity_name, &json);

            // Guard COD_003: colisión de fingerprint
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

            for attr in &model.attributes {
                // Generar DJB2 hash in-line o usar Datom hash logic
                let id = crate::eav::types::datom::Datom::hash_attr_name(&attr.name);
                by_attr_id.insert(id, attr.name.clone());
            }

            by_hash.insert(fingerprint.clone(), entity_name.clone());
            by_entity.insert(entity_name, RegistryEntry { model, fingerprint });
        }

        let registry = CodeRegistry {
            by_entity,
            by_attr_id,
            locales,
        };

        // Post-build: validar scope providers
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
    pub fn get_model(&self, entity_type: &str) -> Option<&EntityModel> {
        self.by_entity.get(entity_type).map(|e| &e.model)
    }

    /// Obtiene el engine channel de una entidad.
    pub fn get_engine(&self, entity_type: &str) -> Option<&EngineChannel> {
        self.by_entity.get(entity_type).map(|e| &e.model.engine)
    }

    /// Obtiene los atributos de una entidad.
    pub fn get_attributes(&self, entity_type: &str) -> Option<&[AttributeDescriptor]> {
        self.by_entity
            .get(entity_type)
            .map(|e| e.model.attributes.as_slice())
    }

    pub fn get_attribute(
        &self,
        entity_type: &str,
        attr_name: &str,
    ) -> Option<&AttributeDescriptor> {
        self.get_attributes(entity_type)
            .and_then(|attrs| attrs.iter().find(|a| a.name == attr_name))
    }

    /// Obtiene el nombre del atributo a partir de su ID DJB2 (O(1))
    pub fn get_attr_name(&self, attr_id: u16) -> Option<&str> {
        self.by_attr_id.get(&attr_id).map(|s| s.as_str())
    }

    /// Obtiene el fingerprint SHA-256.
    pub fn get_fingerprint(&self, entity_type: &str) -> Option<&str> {
        self.by_entity
            .get(entity_type)
            .map(|e| e.fingerprint.as_str())
    }

    /// Lista todos los nombres de entidades registradas.
    pub fn entity_names(&self) -> impl Iterator<Item = &str> {
        self.by_entity.keys().map(|s| s.as_str())
    }

    /// Total de entidades registradas.
    pub fn entity_count(&self) -> usize {
        self.by_entity.len()
    }

    /// Obtiene una etiqueta localizada para una entidad o atributo.
    /// Soporta códigos locales compuestos (ej: "es-CO" -> "es").
    pub fn get_localized_label(
        &self,
        locale: &str,
        entity: &str,
        attr: Option<&str>,
    ) -> Option<String> {
        let clean_locale = locale.split('-').next().unwrap_or(locale);
        let doc = self.locales.get(clean_locale)?;

        if let Some(attr_name) = attr {
            doc.pointer(&format!("/entities/{}/attributes/{}", entity, attr_name))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        } else {
            doc.pointer(&format!("/entities/{}/label", entity))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        }
    }

    /// Obtiene las traducciones de enum localizadas para un atributo específico.
    /// Retorna un `HashMap<String, String>` con todas las traducciones.
    pub fn get_localized_enum_labels(
        &self,
        locale: &str,
        entity: &str,
        attr: &str,
    ) -> std::collections::HashMap<String, String> {
        let clean_locale = locale.split('-').next().unwrap_or(locale);
        let mut labels = std::collections::HashMap::new();
        if let Some(doc) = self.locales.get(clean_locale) {
            if let Some(enum_map) = doc
                .pointer(&format!("/enums/{}/{}", entity, attr))
                .and_then(|v| v.as_object())
            {
                for (k, v) in enum_map {
                    if let Some(val_str) = v.as_str() {
                        labels.insert(k.clone(), val_str.to_string());
                    }
                }
            }
        }
        labels
    }

    // ── Validación post-build ────────────────────────────────────────────────

    /// Valida las specs de materialización contra el grafo ya cargado:
    /// entidades, atributos, generadores y hooks referenciados deben existir.
    /// Fail-fast — una spec rota no despliega (COD_MAT_001).
    /// Valida que los atributos con is_sequence_scope apunten a un
    /// entityRef que sea is_sequence_scope_provider: true.
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

// ── Registro estático global — equivale al `(atom {})` de el stack anterior ────────────
/// OnceLock garantiza que se inicialice UNA sola vez en el cold start.
static REGISTRY: OnceLock<CodeRegistry> = OnceLock::new();

/// Inicializa el registry global. Llamado UNA sola vez en main.rs.
#[allow(clippy::panic)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
pub fn init_global(registry: CodeRegistry) {
    REGISTRY.set(registry).unwrap_or_else(|_| {
        panic!("CodeRegistry ya fue inicializado — no llamar init_global dos veces")
    });
    info!("Códice: registry global activo");
}

pub fn global_opt() -> Option<&'static CodeRegistry> {
    REGISTRY.get()
}

/// Accede al registry global. Panics si no fue inicializado.
#[allow(clippy::expect_used)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
pub fn global() -> &'static CodeRegistry {
    REGISTRY
        .get()
        .expect("CodeRegistry no inicializado — llamar init_global primero")
}

// ── Helpers privados ─────────────────────────────────────────────────────────

/// Genera fingerprint SHA-256 del modelo.
fn schema_fingerprint(entity_name: &str, json: &serde_json::Value) -> String {
    let attr_names: Vec<&str> = json["attributes"]
        .as_array()
        .map(|attrs| attrs.iter().filter_map(|a| a["name"].as_str()).collect())
        .unwrap_or_default();

    let input = format!("{entity_name}{attr_names:?}");
    let hash = Sha256::digest(input.as_bytes());
    hex::encode(hash)
}

/// Parsea un modelo JSON en un EntityModel tipado.
fn parse_entity_model(json: &serde_json::Value) -> Result<EntityModel, DomainError> {
    let entity = json["entity"].as_str().unwrap_or("unknown").to_string();

    let engine = EngineChannel::from(json["engine"].as_str().unwrap_or("oltp"));

    let attributes: Vec<AttributeDescriptor> = json["attributes"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|a| AttributeDescriptor {
            name: a["name"].as_str().unwrap_or("").to_string(),
            attr_type: AttrType::from(a["type"].as_str().unwrap_or("string")),
            label: a["label"].as_str().map(str::to_string),
            required: a["required"].as_bool().unwrap_or(false),
            unique: a["unique"].as_str().map(str::to_string),
            indexed: a["index"].as_bool().unwrap_or(false),
            fts: a["fts"].as_bool().unwrap_or(false),
            is_dimension: a["is_dimension"].as_bool().unwrap_or(false),
            is_metric: a["is_metric"].as_bool().unwrap_or(false),
            entity_ref: a["entityRef"].as_str().map(str::to_string),
            options: a["options"]
                .as_array()
                .map(|o| {
                    o.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
            is_sequence_scope: a["is_sequence_scope"].as_bool().unwrap_or(false),
            is_sequence_scope_via: a["is_sequence_scope_via"].as_bool().unwrap_or(false),
            sensitive: a["sensitive"].as_bool().unwrap_or(false),
            auto_generate: a.get("auto_generate").cloned(),
            validation_regex: a["pattern"].as_str().map(str::to_string),
            default_value: a["default_value"].as_str().map(str::to_string),
        })
        .collect();

    let mut fts_fields: Vec<String> = json["fts_fields"]
        .as_array()
        .map(|f| {
            f.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    if fts_fields.is_empty() {
        for attr in &attributes {
            if attr.fts {
                fts_fields.push(attr.name.clone());
            }
        }
    }

    let event_rules = json["event_rules"].as_array().cloned().unwrap_or_default();

    let write_path_locked = json["routing"]["write_path_locked"]
        .as_bool()
        .unwrap_or(false);
    let is_system = json["is_system"].as_bool().unwrap_or(false);

    Ok(EntityModel {
        entity,
        label: json["label"].as_str().map(str::to_string),
        icon: json["icon"].as_str().map(str::to_string),
        primary_key: json["primary_key"].as_str().map(str::to_string),
        fts_fields,
        engine,
        attributes,
        event_rules,
        is_sequence_scope_provider: json["is_sequence_scope_provider"]
            .as_bool()
            .unwrap_or(false),
        write_path_locked,
        is_system,
        disable_eda: json["disable_eda"].as_bool().unwrap_or(false),
        shadow_sagas_mapping: json
            .get("shadow_sagas_mapping")
            .filter(|v| v.is_object())
            .cloned(),
        constraints: parse_constraints(json),
    })
}

/// Lee `constraints` del JSON del modelo.
///
/// Una restricción malformada se descarta con un aviso en lugar de impedir el
/// arranque: un modelo mal escrito no debe tumbar el motor entero, y el aviso
/// dice exactamente qué se ignoró.
fn parse_constraints(json: &serde_json::Value) -> Vec<Constraint> {
    let Some(items) = json["constraints"].as_array() else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|c| {
            let tipo = c["type"].as_str().unwrap_or("unique");
            let parse_pares = |obj: &serde_json::Value| -> Option<Vec<(String, String)>> {
                let map = obj.as_object()?;
                Some(
                    map.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect(),
                )
            };

            let (kind, attributes, when) = match tipo {
                "unique" => (
                    ConstraintKind::Unique,
                    c["attributes"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default(),
                    None,
                ),
                "requires_when" => (
                    ConstraintKind::RequiresWhen,
                    c["required"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default(),
                    c["when"]
                        .as_object()
                        .and_then(|w| parse_pares(&json_clone(w))),
                ),
                "at_most" => (
                    ConstraintKind::AtMost,
                    ["field", "of"]
                        .iter()
                        .filter_map(|k| c[*k].as_str().map(str::to_string))
                        .collect(),
                    None,
                ),
                "at_least" => (
                    ConstraintKind::AtLeast,
                    ["field", "of"]
                        .iter()
                        .filter_map(|k| c[*k].as_str().map(str::to_string))
                        .collect(),
                    None,
                ),
                "requires_any" => (
                    ConstraintKind::RequiresAny,
                    c["any"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default(),
                    None,
                ),
                "ref_state" => (
                    ConstraintKind::RefState,
                    c["attr"]
                        .as_str()
                        .map(|s| vec![s.to_string()])
                        .unwrap_or_default(),
                    c["conditions"]
                        .as_object()
                        .and_then(|w| parse_pares(&json_clone(w))),
                ),
                other => {
                    tracing::warn!("[Codice] restricción de tipo desconocido '{other}'; se ignora");
                    return None;
                }
            };

            if attributes.is_empty() {
                tracing::warn!("[Codice] restricción sin atributos; se ignora");
                return None;
            }

            if matches!(
                kind,
                ConstraintKind::RequiresWhen | ConstraintKind::RefState
            ) && when.as_ref().is_none_or(|w| w.is_empty())
            {
                tracing::warn!(
                    "[Codice] restricción {tipo} sin condiciones 'when'/'conditions'; se ignora"
                );
                return None;
            }

            Some(Constraint {
                kind,
                scope: ConstraintScope::from(c["scope"].as_str().unwrap_or("tenant")),
                attributes,
                when,
            })
        })
        .collect()
}

/// Clona un mapa de serde_json como Value para el parser de pares.
fn json_clone(w: &serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    serde_json::Value::Object(w.clone())
}

/// Recolecta todos los archivos .json de un directorio.
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

#[cfg(test)]
#[path = "tests/registry_tests.rs"]
mod tests;
