#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::missing_const_for_fn)]

pub mod jet {
    tonic::include_proto!("jet");
}

pub mod metrics {
    tonic::include_proto!("metrics");
}
