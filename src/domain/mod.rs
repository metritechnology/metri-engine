pub mod errors;
pub mod protocols;
pub mod pipeline;
pub mod audit;
pub mod error_catalog;

pub use errors::{DomainError, ErrorCode};
pub use protocols::DomainResult;
pub use error_catalog::{ErrorCatalog, ErrorEntry, init_global as init_error_catalog, global as error_catalog};
