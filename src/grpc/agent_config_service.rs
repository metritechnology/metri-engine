//! AgentConfigService gRPC implementation.
//!
//! Implementación de gRPC para AgentConfigService
//!
//! Responsabilidades:
//! 1. Servir las configuraciones dinámicas de módulos y prompts al agente de IA.
//! 2. Resolver locale (es/en) y nivel de detalle (full/compact) para los archivos de prompt.
//! 3. Retornar las rutas de navegación declaradas de forma centralizada.

use serde::Deserialize;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use super::pb::agent_config_service_server::AgentConfigService;
use super::pb::{
    AgentConfigRequest, AgentConfigResponse, AgentModuleConfig, NavigationRouteConfig,
    Status as PbStatus,
};

#[derive(Debug, Deserialize)]
struct RawModule {
    name: String,
    description: String,
    keywords: Vec<String>,
    tools: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawRoute {
    route: String,
    label_es: String,
    label_en: String,
    keywords: Vec<String>,
}

pub struct AgentConfigServiceImpl {
    prompts_dir: String,
}

impl Default for AgentConfigServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentConfigServiceImpl {
    pub fn new() -> Self {
        let prompts_dir =
            std::env::var("PROMPTS_DIR").unwrap_or_else(|_| "config/prompts".to_string());
        info!(
            "[AgentConfigService] Inicializando con directorio de prompts: {}",
            prompts_dir
        );
        Self { prompts_dir }
    }
}

fn resolve_prompt_text(prompts_dir: &str, name: &str, locale: &str, level: &str) -> String {
    let module_dir = std::path::Path::new(prompts_dir).join(name);

    // Para nivel "compact", intentar cargar el archivo compact correspondiente primero
    if level == "compact" {
        let compact_filename = format!("{}_compact.txt", locale);
        let path = module_dir.join(&compact_filename);
        if let Ok(content) = std::fs::read_to_string(&path) {
            return content;
        }
    }

    // De lo contrario, o si falta el compact, usar versión completa
    let filename = format!("{}.txt", locale);
    let path = module_dir.join(&filename);
    match std::fs::read_to_string(&path) {
        Ok(content) => content,
        Err(_) => {
            // Fallback a versión en español (default "es")
            let fallback_filename = if level == "compact" {
                "es_compact.txt"
            } else {
                "es.txt"
            };
            let fallback_path = module_dir.join(fallback_filename);
            if let Ok(content) = std::fs::read_to_string(&fallback_path) {
                return content;
            }
            warn!(
                "Archivo de prompt no encontrado para módulo {} (locale: {}, level: {})",
                name, locale, level
            );
            String::new()
        }
    }
}

#[tonic::async_trait]
impl AgentConfigService for AgentConfigServiceImpl {
    async fn get_agent_config(
        &self,
        request: Request<AgentConfigRequest>,
    ) -> Result<Response<AgentConfigResponse>, Status> {
        let req = request.into_inner();
        let locale = if req.locale.is_empty() {
            "es"
        } else {
            &req.locale
        };
        let prompt_level = if req.prompt_level.is_empty() {
            "full"
        } else {
            &req.prompt_level
        };

        info!(
            locale = %locale,
            prompt_level = %prompt_level,
            "[AgentConfigService] Procesando GetAgentConfig"
        );

        // 1. Resolver el prompt base
        let base_text = resolve_prompt_text(&self.prompts_dir, "base", locale, prompt_level);
        let base_prompt = Some(AgentModuleConfig {
            name: "base".to_string(),
            description: "Base system prompt".to_string(),
            keywords: vec![],
            prompt_text: base_text,
            tools: vec![],
            is_core: true,
        });

        // 2. Cargar módulos desde config.json
        let config_path = std::path::Path::new(&self.prompts_dir).join("config.json");
        let modules_config_raw = std::fs::read_to_string(&config_path)
            .map_err(|e| Status::internal(format!("Error leyendo prompts config.json: {}", e)))?;

        let raw_modules: Vec<RawModule> = serde_json::from_str(&modules_config_raw)
            .map_err(|e| Status::internal(format!("Error de parseo en config.json: {}", e)))?;

        let mut modules = Vec::new();
        for rm in raw_modules {
            let text = resolve_prompt_text(&self.prompts_dir, &rm.name, locale, prompt_level);
            // navigation, form_autofill y deletions se consideran módulos core
            let is_core =
                rm.name == "navigation" || rm.name == "form_autofill" || rm.name == "deletions";
            modules.push(AgentModuleConfig {
                name: rm.name,
                description: rm.description,
                keywords: rm.keywords,
                prompt_text: text,
                tools: rm.tools,
                is_core,
            });
        }

        // 3. Cargar rutas de navegación desde navigation_routes.json
        let routes_path = std::path::Path::new(&self.prompts_dir).join("navigation_routes.json");
        let routes_config_raw = std::fs::read_to_string(&routes_path).map_err(|e| {
            Status::internal(format!("Error leyendo navigation_routes.json: {}", e))
        })?;

        let raw_routes: Vec<RawRoute> = serde_json::from_str(&routes_config_raw).map_err(|e| {
            Status::internal(format!("Error de parseo en navigation_routes.json: {}", e))
        })?;

        let routes = raw_routes
            .into_iter()
            .map(|rr| NavigationRouteConfig {
                route: rr.route,
                label_es: rr.label_es,
                label_en: rr.label_en,
                keywords: rr.keywords,
            })
            .collect();

        let response = AgentConfigResponse {
            status: Some(PbStatus {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            base_prompt,
            modules,
            routes,
        };

        Ok(Response::new(response))
    }
}
