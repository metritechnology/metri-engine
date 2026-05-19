pub mod pb {
    tonic::include_proto!("metri");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("metri_descriptor");
}

pub mod service;
pub mod translator;
pub mod interceptors;
pub mod server;
