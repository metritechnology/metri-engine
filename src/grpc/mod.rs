pub mod pb {
    tonic::include_proto!("metri");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("metri_descriptor");

    pub mod eda {
        pub mod v1 {
            tonic::include_proto!("metri.eda.v1");
        }
    }
}

pub mod agent_config_service;
pub mod bootstrap;
pub mod eda_service;
pub(crate) mod handlers;
pub mod interceptors;
pub mod quota_service;
pub mod server;
pub mod service;
pub mod translator;
