//! Wire compatibility tests to ensure wincode serialization matches bincode exactly.
//!
//! These tests verify that our wincode implementation produces identical wire format
//! to bincode for Solana transactions, ensuring backward compatibility.

#![cfg(feature = "wincode")]

use {
    bincode::config::Options,
    solana_hash::Hash,
    solana_message::{
        MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction, legacy,
        v0::MessageAddressTableLookup,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    std::mem::MaybeUninit,
    wincode::{SchemaRead, SchemaWrite, io::Cursor},
    yellowstone_jet::wincode_schema::{
        HashSchema, PubkeySchema, SigChallengeSchema, SignatureSchema, SignedSigChallengeSchema,
        deserialize_transaction, deserialize_transaction_with_limit, serialize_transaction,
    },
};

/// Helper to create a simple legacy transaction for testing
fn create_legacy_transaction() -> VersionedTransaction {
    let payer = Pubkey::new_unique();
    let to = Pubkey::new_unique();
    let program_id = Pubkey::new_unique();

    let message = legacy::Message {
        header: MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        },
        account_keys: vec![payer, to, program_id],
        recent_blockhash: Hash::new_unique(),
        instructions: vec![CompiledInstruction {
            program_id_index: 2,
            accounts: vec![0, 1],
            data: vec![1, 2, 3, 4],
        }],
    };

    VersionedTransaction {
        signatures: vec![Signature::new_unique()],
        message: VersionedMessage::Legacy(message),
    }
}

/// Helper to create a V0 transaction with address table lookups
fn create_v0_transaction() -> VersionedTransaction {
    let payer = Pubkey::new_unique();
    let to = Pubkey::new_unique();
    let program_id = Pubkey::new_unique();
    let lookup_table = Pubkey::new_unique();

    let message = solana_message::v0::Message {
        header: MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 2,
        },
        account_keys: vec![payer, to, program_id],
        recent_blockhash: Hash::new_unique(),
        instructions: vec![
            CompiledInstruction {
                program_id_index: 2,
                accounts: vec![0, 1, 3],
                data: vec![10, 20, 30],
            },
            CompiledInstruction {
                program_id_index: 2,
                accounts: vec![0, 4],
                data: vec![],
            },
        ],
        address_table_lookups: vec![MessageAddressTableLookup {
            account_key: lookup_table,
            writable_indexes: vec![0, 1],
            readonly_indexes: vec![2],
        }],
    };

    VersionedTransaction {
        signatures: vec![Signature::new_unique(), Signature::new_unique()],
        message: VersionedMessage::V0(message),
    }
}

/// Get bincode serialization for comparison (using Solana's exact config)
fn bincode_serialize_transaction(tx: &VersionedTransaction) -> Vec<u8> {
    // Use the same config as solana_bincode::serialize
    bincode::options()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .serialize(tx)
        .expect("bincode serialize")
}

/// Test: Legacy transaction serialization matches bincode exactly
#[test]
fn test_legacy_transaction_wire_compat() {
    let tx = create_legacy_transaction();

    let bincode_bytes = bincode_serialize_transaction(&tx);
    let wincode_bytes = serialize_transaction(&tx).expect("wincode serialize");

    assert_eq!(
        bincode_bytes.len(),
        wincode_bytes.len(),
        "Length mismatch: bincode={}, wincode={}",
        bincode_bytes.len(),
        wincode_bytes.len()
    );

    assert_eq!(
        bincode_bytes, wincode_bytes,
        "Wire format mismatch for legacy transaction"
    );
}

/// Test: V0 transaction serialization matches bincode exactly
#[test]
fn test_v0_transaction_wire_compat() {
    let tx = create_v0_transaction();

    let bincode_bytes = bincode_serialize_transaction(&tx);
    let wincode_bytes = serialize_transaction(&tx).expect("wincode serialize");

    assert_eq!(
        bincode_bytes.len(),
        wincode_bytes.len(),
        "Length mismatch: bincode={}, wincode={}",
        bincode_bytes.len(),
        wincode_bytes.len()
    );

    assert_eq!(
        bincode_bytes, wincode_bytes,
        "Wire format mismatch for V0 transaction"
    );
}

/// Test: Deserialization roundtrip for legacy transactions
#[test]
fn test_legacy_transaction_roundtrip() {
    let original = create_legacy_transaction();
    let bytes = serialize_transaction(&original).expect("serialize");
    let deserialized = deserialize_transaction(&bytes).expect("deserialize");

    assert_eq!(original.signatures, deserialized.signatures);
    assert_eq!(original.message, deserialized.message);
}

/// Test: Deserialization roundtrip for V0 transactions
#[test]
fn test_v0_transaction_roundtrip() {
    let original = create_v0_transaction();
    let bytes = serialize_transaction(&original).expect("serialize");
    let deserialized = deserialize_transaction(&bytes).expect("deserialize");

    assert_eq!(original.signatures, deserialized.signatures);
    assert_eq!(original.message, deserialized.message);
}

/// Test: Wincode can deserialize bincode-encoded legacy transactions
#[test]
fn test_wincode_reads_bincode_legacy() {
    let original = create_legacy_transaction();
    let bincode_bytes = bincode_serialize_transaction(&original);
    let deserialized =
        deserialize_transaction(&bincode_bytes).expect("deserialize bincode with wincode");

    assert_eq!(original.signatures, deserialized.signatures);
    assert_eq!(original.message, deserialized.message);
}

/// Test: Wincode can deserialize bincode-encoded V0 transactions
#[test]
fn test_wincode_reads_bincode_v0() {
    let original = create_v0_transaction();
    let bincode_bytes = bincode_serialize_transaction(&original);
    let deserialized =
        deserialize_transaction(&bincode_bytes).expect("deserialize bincode with wincode");

    assert_eq!(original.signatures, deserialized.signatures);
    assert_eq!(original.message, deserialized.message);
}

/// Test: Bincode can deserialize wincode-encoded legacy transactions
#[test]
fn test_bincode_reads_wincode_legacy() {
    let original = create_legacy_transaction();
    let wincode_bytes = serialize_transaction(&original).expect("serialize");

    let deserialized: VersionedTransaction = bincode::options()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize(&wincode_bytes)
        .expect("deserialize wincode with bincode");

    assert_eq!(original.signatures, deserialized.signatures);
    assert_eq!(original.message, deserialized.message);
}

/// Test: Bincode can deserialize wincode-encoded V0 transactions
#[test]
fn test_bincode_reads_wincode_v0() {
    let original = create_v0_transaction();
    let wincode_bytes = serialize_transaction(&original).expect("serialize");

    let deserialized: VersionedTransaction = bincode::options()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize(&wincode_bytes)
        .expect("deserialize wincode with bincode");

    assert_eq!(original.signatures, deserialized.signatures);
    assert_eq!(original.message, deserialized.message);
}

/// Test: Size limit enforcement
#[test]
fn test_size_limit_enforcement() {
    let tx = create_legacy_transaction();
    let bytes = serialize_transaction(&tx).expect("serialize");

    // With sufficient limit, should succeed
    let (_, consumed) =
        deserialize_transaction_with_limit(&bytes, bytes.len()).expect("deserialize within limit");
    assert_eq!(consumed, bytes.len());

    // With insufficient limit, should fail
    let result = deserialize_transaction_with_limit(&bytes, 10);
    assert!(result.is_err(), "Should fail with small limit");
}

/// Test: Trailing bytes are allowed (matching bincode behavior)
#[test]
fn test_trailing_bytes_allowed() {
    let tx = create_legacy_transaction();
    let mut bytes = serialize_transaction(&tx).expect("serialize");

    // Add trailing garbage
    bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF]);

    // Should still deserialize successfully
    let deserialized = deserialize_transaction(&bytes).expect("deserialize with trailing bytes");
    assert_eq!(tx.signatures, deserialized.signatures);
    assert_eq!(tx.message, deserialized.message);
}

/// Test: PubkeySchema roundtrip
#[test]
fn test_pubkey_schema_roundtrip() {
    let pk = Pubkey::new_unique();
    let schema = PubkeySchema::from(pk);

    let size = PubkeySchema::size_of(&schema).unwrap();
    let mut buf = vec![0u8; size];
    PubkeySchema::write(&mut buf.as_mut_slice(), &schema).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let mut dst = MaybeUninit::uninit();
    PubkeySchema::read(&mut cursor, &mut dst).unwrap();
    let decoded = unsafe { dst.assume_init() };

    assert_eq!(pk, Pubkey::from(decoded));
}

/// Test: HashSchema roundtrip
#[test]
fn test_hash_schema_roundtrip() {
    let h = Hash::new_unique();
    let schema = HashSchema::from(h);

    let size = HashSchema::size_of(&schema).unwrap();
    let mut buf = vec![0u8; size];
    HashSchema::write(&mut buf.as_mut_slice(), &schema).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let mut dst = MaybeUninit::uninit();
    HashSchema::read(&mut cursor, &mut dst).unwrap();
    let decoded = unsafe { dst.assume_init() };

    assert_eq!(h, Hash::from(decoded));
}

/// Test: SignatureSchema roundtrip
#[test]
fn test_signature_schema_roundtrip() {
    let sig = Signature::new_unique();
    let schema = SignatureSchema::from(sig);

    let size = SignatureSchema::size_of(&schema).unwrap();
    let mut buf = vec![0u8; size];
    SignatureSchema::write(&mut buf.as_mut_slice(), &schema).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let mut dst = MaybeUninit::uninit();
    SignatureSchema::read(&mut cursor, &mut dst).unwrap();
    let decoded = unsafe { dst.assume_init() };

    assert_eq!(sig, Signature::from(decoded));
}

/// Test: SigChallengeSchema roundtrip
#[test]
fn test_sig_challenge_schema_roundtrip() {
    let schema = SigChallengeSchema {
        pubkey_to_verify: PubkeySchema::from(Pubkey::new_unique()),
        timestamp: 1234567890,
        nonce: vec![1, 2, 3, 4, 5],
    };

    let bytes = schema.to_bytes().expect("serialize");

    let mut cursor = Cursor::new(&bytes[..]);
    let mut dst = MaybeUninit::uninit();
    SigChallengeSchema::read(&mut cursor, &mut dst).unwrap();
    let decoded = unsafe { dst.assume_init() };

    assert_eq!(schema.pubkey_to_verify, decoded.pubkey_to_verify);
    assert_eq!(schema.timestamp, decoded.timestamp);
    assert_eq!(schema.nonce, decoded.nonce);
}

/// Test: SignedSigChallengeSchema roundtrip
#[test]
fn test_signed_sig_challenge_schema_roundtrip() {
    let schema = SignedSigChallengeSchema {
        signature: SignatureSchema::from(Signature::new_unique()),
        payload: vec![10, 20, 30, 40],
    };

    let bytes = schema.to_bytes().expect("serialize");

    let mut cursor = Cursor::new(&bytes[..]);
    let mut dst = MaybeUninit::uninit();
    SignedSigChallengeSchema::read(&mut cursor, &mut dst).unwrap();
    let decoded = unsafe { dst.assume_init() };

    assert_eq!(schema.signature, decoded.signature);
    assert_eq!(schema.payload, decoded.payload);
}

/// Test: Empty transaction (minimal valid transaction)
#[test]
fn test_minimal_transaction() {
    let message = legacy::Message {
        header: MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        },
        account_keys: vec![Pubkey::new_unique()],
        recent_blockhash: Hash::new_unique(),
        instructions: vec![],
    };

    let tx = VersionedTransaction {
        signatures: vec![Signature::new_unique()],
        message: VersionedMessage::Legacy(message),
    };

    let bincode_bytes = bincode_serialize_transaction(&tx);
    let wincode_bytes = serialize_transaction(&tx).expect("wincode serialize");

    assert_eq!(bincode_bytes, wincode_bytes);

    let deserialized = deserialize_transaction(&wincode_bytes).expect("deserialize");
    assert_eq!(tx.signatures, deserialized.signatures);
    assert_eq!(tx.message, deserialized.message);
}

/// Test: Transaction with many signatures
#[test]
fn test_many_signatures() {
    let message = legacy::Message {
        header: MessageHeader {
            num_required_signatures: 5,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        },
        account_keys: (0..5).map(|_| Pubkey::new_unique()).collect(),
        recent_blockhash: Hash::new_unique(),
        instructions: vec![],
    };

    let tx = VersionedTransaction {
        signatures: (0..5).map(|_| Signature::new_unique()).collect(),
        message: VersionedMessage::Legacy(message),
    };

    let bincode_bytes = bincode_serialize_transaction(&tx);
    let wincode_bytes = serialize_transaction(&tx).expect("wincode serialize");

    assert_eq!(bincode_bytes, wincode_bytes);
}

/// Test: Transaction with large instruction data
#[test]
fn test_large_instruction_data() {
    let message = legacy::Message {
        header: MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        },
        account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique()],
        recent_blockhash: Hash::new_unique(),
        instructions: vec![CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: vec![0xAB; 500], // Large data
        }],
    };

    let tx = VersionedTransaction {
        signatures: vec![Signature::new_unique()],
        message: VersionedMessage::Legacy(message),
    };

    let bincode_bytes = bincode_serialize_transaction(&tx);
    let wincode_bytes = serialize_transaction(&tx).expect("wincode serialize");

    assert_eq!(bincode_bytes, wincode_bytes);
}

/// Test: V0 transaction with multiple address table lookups
#[test]
fn test_multiple_address_table_lookups() {
    let message = solana_message::v0::Message {
        header: MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        },
        account_keys: vec![Pubkey::new_unique()],
        recent_blockhash: Hash::new_unique(),
        instructions: vec![],
        address_table_lookups: vec![
            MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![0, 1, 2],
                readonly_indexes: vec![3, 4],
            },
            MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![],
                readonly_indexes: vec![0, 1, 2, 3, 4, 5],
            },
            MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![10, 20, 30],
                readonly_indexes: vec![],
            },
        ],
    };

    let tx = VersionedTransaction {
        signatures: vec![Signature::new_unique()],
        message: VersionedMessage::V0(message),
    };

    let bincode_bytes = bincode_serialize_transaction(&tx);
    let wincode_bytes = serialize_transaction(&tx).expect("wincode serialize");

    assert_eq!(bincode_bytes, wincode_bytes);
}
