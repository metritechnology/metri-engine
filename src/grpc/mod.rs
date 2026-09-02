pub mod pb {
    tonic::include_proto!("metri");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("metri_descriptor");

    pub mod eda {
        pub mod v1 {
            tonic::include_proto!("metri.eda.v1");
        }
    }
}

pub mod service;
pub mod eda_service;
pub mod quota_service;
pub mod agent_config_service;
pub mod translator;
pub mod interceptors;
pub mod server;
pub mod bootstrap;

