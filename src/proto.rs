#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::missing_const_for_fn)]

pub mod jet {
    tonic::include_proto!("jet");
}

pub mod metrics {
    tonic::include_proto!("metrics");
}

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod block_engine {
    tonic::include_proto!("block_engine");
}

pub mod packet {
    tonic::include_proto!("packet");
}

pub mod shared {
    tonic::include_proto!("shared");
}

pub mod bundle {
    tonic::include_proto!("bundle");
}
