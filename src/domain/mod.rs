pub mod audit;
pub mod error_catalog;
pub mod errors;
pub mod pipeline;
pub mod protocols;

pub use error_catalog::{
    global as error_catalog, init_global as init_error_catalog, ErrorCatalog, ErrorEntry,
};
pub use errors::{DomainError, ErrorCode};
pub use protocols::DomainResult;
