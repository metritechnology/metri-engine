//! Single source of truth for reserved system attribute ids.
//!
//! Fuente única de los atributos de sistema.
//!
//! Los primeros attr_ids del motor están reservados y su nombre va ligado al
//! número: el escritor los emite (enricher) y el lector de atributos activos
//! los resuelve de vuelta (transact::get_active_attributes). Antes de este
//! módulo, ambos lados repetían los literales a mano — una drift silenciosa
//! entre escritura y lectura habría corrompido el resolve sin que compile
//! fallara. Hoy ambos lados consumen estas constantes.

/// `entity/ulid` — identidad de la entidad.
pub const ULID_ID: u16 = 0x0000;
/// `entity/type` — tipo de la entidad (formato con slash).
pub const TYPE_ID: u16 = 0x0001;
/// `tenant/id` — aislamiento multitenant.
pub const TENANT_ID: u16 = 0x0002;
/// `meta/created_at` — epoch ms de creación.
pub const CREATED_AT_ID: u16 = 0x0003;
/// `meta/updated_at` — epoch ms de última modificación.
pub const UPDATED_AT_ID: u16 = 0x0004;

pub const ULID_NAME: &str = "entity/ulid";
pub const TYPE_NAME: &str = "entity/type";
pub const TENANT_NAME: &str = "tenant/id";
pub const CREATED_AT_NAME: &str = "meta/created_at";
pub const UPDATED_AT_NAME: &str = "meta/updated_at";

/// Resuelve el nombre de un atributo de sistema por su attr_id reservado.
/// `None` para ids no reservados — el caller resuelve esos contra el Códice.
pub fn name_of(attr_id: u16) -> Option<&'static str> {
    match attr_id {
        ULID_ID => Some(ULID_NAME),
        TYPE_ID => Some(TYPE_NAME),
        TENANT_ID => Some(TENANT_NAME),
        CREATED_AT_ID => Some(CREATED_AT_NAME),
        UPDATED_AT_ID => Some(UPDATED_AT_NAME),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn los_cinco_ids_reservados_resuelven_su_nombre() {
        assert_eq!(name_of(0), Some("entity/ulid"));
        assert_eq!(name_of(1), Some("entity/type"));
        assert_eq!(name_of(2), Some("tenant/id"));
        assert_eq!(name_of(3), Some("meta/created_at"));
        assert_eq!(name_of(4), Some("meta/updated_at"));
    }

    #[test]
    fn un_id_no_reservado_no_resuelve() {
        assert_eq!(name_of(5), None);
        assert_eq!(name_of(u16::MAX), None);
    }

    #[test]
    fn las_constantes_emparejan_id_y_nombre() {
        assert_eq!(name_of(ULID_ID), Some(ULID_NAME));
        assert_eq!(name_of(TYPE_ID), Some(TYPE_NAME));
        assert_eq!(name_of(TENANT_ID), Some(TENANT_NAME));
        assert_eq!(name_of(CREATED_AT_ID), Some(CREATED_AT_NAME));
        assert_eq!(name_of(UPDATED_AT_ID), Some(UPDATED_AT_NAME));
    }
}
