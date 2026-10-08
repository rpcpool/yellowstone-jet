//! Transactions handed to the driver and the caller-supplied metadata they carry.

include!(concat!(env!("OUT_DIR"), "/txn_info_cap.rs"));

use {bytes::Bytes, core::fmt, solana_pubkey::Pubkey, std::any::TypeId};

#[repr(align(16))]
#[derive(Clone, Copy)]
struct TxnInfoStorage([u8; TXN_INFO_CAP]);

#[derive(Clone, Copy)]
pub struct TpuSenderTxnInfo {
    inner: TxnInfoStorage,
    type_id: TypeId,
}

impl fmt::Debug for TpuSenderTxnInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpuSenderTxnInfo")
            .field("type_id", &self.type_id)
            .finish()
    }
}

impl TpuSenderTxnInfo {
    pub fn new<T: Sized + Copy + 'static>(val: T) -> Self {
        assert!(
            std::mem::size_of::<T>() <= TXN_INFO_CAP,
            "TpuSenderTxnInfo can only hold up to {} bytes, but T is {} bytes",
            TXN_INFO_CAP,
            std::mem::size_of::<T>()
        );
        let mut storage = TxnInfoStorage([0u8; TXN_INFO_CAP]);
        unsafe {
            std::ptr::copy_nonoverlapping(
                (&val as *const T).cast::<u8>(),
                storage.0.as_mut_ptr(),
                std::mem::size_of::<T>(),
            );
        }
        Self {
            inner: storage,
            type_id: TypeId::of::<T>(),
        }
    }

    pub fn downcast_ref<T: Sized + Copy + 'static>(&self) -> Option<&T> {
        let size = std::mem::size_of::<T>();
        assert!(
            size <= TXN_INFO_CAP,
            "TpuSenderTxnInfo can only hold up to {} bytes, but T is {} bytes",
            TXN_INFO_CAP,
            size
        );

        if self.type_id != TypeId::of::<T>() {
            return None;
        }

        let bytes = &self.inner.0;
        let ptr = bytes.as_ptr();
        let align = std::mem::align_of::<T>();
        if !(ptr as usize).is_multiple_of(align) {
            return None;
        }

        let value = unsafe { &*ptr.cast::<T>() };
        Some(value)
    }
}

///
/// A transaction with destination details to be sent to a remote peer.
///
#[derive(Debug)]
pub struct TpuSenderTxn {
    /// The wire format of the transaction.
    pub(crate) wire: Bytes,
    /// The pubkey of the remote peer to send the transaction to.
    pub remote_peer: Pubkey,
    ///
    /// Arbitrary information about the transaction. This can be used to store additional metadata or context about the transaction.
    pub info: Option<TpuSenderTxnInfo>,
}

impl TpuSenderTxn {
    pub const fn from_bytes(
        remote_peer: Pubkey,
        wire: Bytes,
        info: Option<TpuSenderTxnInfo>,
    ) -> Self {
        Self {
            wire,
            remote_peer,
            info,
        }
    }

    pub fn from_owned<T>(remote_peer: Pubkey, wire: T, info: Option<TpuSenderTxnInfo>) -> Self
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        Self {
            wire: Bytes::from_owner(wire),
            remote_peer,
            info,
        }
    }
}

#[cfg(test)]
mod test_tpu_sender_txn_info {
    use crate::core::TpuSenderTxnInfo;

    #[test]
    fn test_txn_info() {
        #[derive(Debug, Clone, PartialEq, Eq, Copy)]
        struct TestTxnInfo {
            data: [u8; 4],
        }

        let expected = TestTxnInfo {
            data: [0xDE, 0xAD, 0xBE, 0xEF],
        };
        let info = TpuSenderTxnInfo::new(expected);

        let actual = info.downcast_ref::<TestTxnInfo>().copied().unwrap();
        assert_eq!(actual, expected);
    }
}
