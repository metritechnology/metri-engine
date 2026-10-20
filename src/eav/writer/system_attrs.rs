//! Fuente única de verdad para los identificadores numéricos (`attr_id`) y nombres reservados de los atributos de sistema.
//!
//! Los primeros `attr_id` del motor (`0x0000..=0x0006`) están reservados y vinculan de forma
//! inmutable su código numérico con su nombre canónico tanto en el enriquecedor de escritura
//! (`enricher`) como en el lector de atributos activos (`transact::get_active_attributes`).

/// `entity/ulid` — identificador numérico reservado (`0x0000`) para la identidad de la entidad.
pub const ULID_ID: u16 = 0x0000;
/// `entity/type` — identificador numérico reservado (`0x0001`) para el tipo de la entidad.
pub const TYPE_ID: u16 = 0x0001;
/// `tenant/id` — identificador numérico reservado (`0x0002`) para el aislamiento multitenant.
pub const TENANT_ID: u16 = 0x0002;
/// `meta/created_at` — identificador numérico reservado (`0x0003`) para el instante de creación (epoch ms).
pub const CREATED_AT_ID: u16 = 0x0003;
/// `meta/updated_at` — identificador numérico reservado (`0x0004`) para el instante de última modificación (epoch ms).
pub const UPDATED_AT_ID: u16 = 0x0004;
/// `meta/created_by` — identificador numérico reservado (`0x0005`) para el actor que creó la entidad.
pub const CREATED_BY_ID: u16 = 0x0005;
/// `meta/updated_by` — identificador numérico reservado (`0x0006`) para el actor de la última modificación.
pub const UPDATED_BY_ID: u16 = 0x0006;

/// Nombre canónico del atributo de sistema para el ULID de la entidad (`"entity/ulid"`).
pub const ULID_NAME: &str = "entity/ulid";
/// Nombre canónico del atributo de sistema para el tipo de entidad (`"entity/type"`).
pub const TYPE_NAME: &str = "entity/type";
/// Nombre canónico del atributo de sistema para el identificador del tenant (`"tenant/id"`).
pub const TENANT_NAME: &str = "tenant/id";
/// Nombre canónico del atributo de sistema para la marca temporal de creación (`"meta/created_at"`).
pub const CREATED_AT_NAME: &str = "meta/created_at";
/// Nombre canónico del atributo de sistema para la marca temporal de actualización (`"meta/updated_at"`).
pub const UPDATED_AT_NAME: &str = "meta/updated_at";
/// Nombre canónico del atributo de sistema para el actor creador (`"meta/created_by"`).
pub const CREATED_BY_NAME: &str = "meta/created_by";
/// Nombre canónico del atributo de sistema para el actor actualizador (`"meta/updated_by"`).
pub const UPDATED_BY_NAME: &str = "meta/updated_by";

/// Resuelve el nombre de un atributo de sistema por su attr_id reservado.
/// `None` para ids no reservados — el caller resuelve esos contra el Códice.
pub fn name_of(attr_id: u16) -> Option<&'static str> {
    match attr_id {
        ULID_ID => Some(ULID_NAME),
        TYPE_ID => Some(TYPE_NAME),
        TENANT_ID => Some(TENANT_NAME),
        CREATED_AT_ID => Some(CREATED_AT_NAME),
        UPDATED_AT_ID => Some(UPDATED_AT_NAME),
        CREATED_BY_ID => Some(CREATED_BY_NAME),
        UPDATED_BY_ID => Some(UPDATED_BY_NAME),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn los_siete_ids_reservados_resuelven_su_nombre() {
        assert_eq!(name_of(0), Some("entity/ulid"));
        assert_eq!(name_of(1), Some("entity/type"));
        assert_eq!(name_of(2), Some("tenant/id"));
        assert_eq!(name_of(3), Some("meta/created_at"));
        assert_eq!(name_of(4), Some("meta/updated_at"));
        assert_eq!(name_of(5), Some("meta/created_by"));
        assert_eq!(name_of(6), Some("meta/updated_by"));
    }

    #[test]
    fn un_id_no_reservado_no_resuelve() {
        assert_eq!(name_of(7), None);
        assert_eq!(name_of(u16::MAX), None);
    }

    #[test]
    fn las_constantes_emparejan_id_y_nombre() {
        assert_eq!(name_of(ULID_ID), Some(ULID_NAME));
        assert_eq!(name_of(TYPE_ID), Some(TYPE_NAME));
        assert_eq!(name_of(TENANT_ID), Some(TENANT_NAME));
        assert_eq!(name_of(CREATED_AT_ID), Some(CREATED_AT_NAME));
        assert_eq!(name_of(UPDATED_AT_ID), Some(UPDATED_AT_NAME));
        assert_eq!(name_of(CREATED_BY_ID), Some(CREATED_BY_NAME));
        assert_eq!(name_of(UPDATED_BY_ID), Some(UPDATED_BY_NAME));
    }
}
