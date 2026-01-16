//! Wincode schema types for Solana transaction serialization.
//!
//! This module provides shadow types that implement wincode's `SchemaWrite`/`SchemaRead`
//! traits for Solana SDK types, preserving exact wire compatibility with bincode.

use {
    solana_hash::Hash,
    solana_message::{
        MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction, legacy,
        v0::MessageAddressTableLookup,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    std::mem::MaybeUninit,
    wincode::{
        ReadError, SchemaRead, SchemaWrite, WriteError,
        error::{ReadResult, WriteResult},
        io::{Cursor, Reader, Writer},
    },
};

/// Type alias for wincode::Error for easier use in function signatures
pub type WincodeError = wincode::Error;

/// A vector with compact-u16 length encoding (1-3 bytes based on value).
/// Matches Solana's `short_vec` encoding used in VersionedTransaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortVec<T>(pub Vec<T>);

impl<T> ShortVec<T> {
    pub const fn new(vec: Vec<T>) -> Self {
        Self(vec)
    }

    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T> From<Vec<T>> for ShortVec<T> {
    fn from(vec: Vec<T>) -> Self {
        Self(vec)
    }
}

impl<T> From<ShortVec<T>> for Vec<T> {
    fn from(short_vec: ShortVec<T>) -> Self {
        short_vec.0
    }
}

/// Calculate compact-u16 encoded length
const fn short_u16_size(val: u16) -> usize {
    if val < 0x80 {
        1
    } else if val < 0x4000 {
        2
    } else {
        3
    }
}

/// Encode a u16 as compact-u16 (1-3 bytes)
fn encode_short_u16(writer: &mut impl Writer, mut val: u16) -> WriteResult<()> {
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            writer.write(&[byte])?;
            return Ok(());
        }
        byte |= 0x80;
        writer.write(&[byte])?;
    }
}

/// Decode a compact-u16 (1-3 bytes) into u16
fn decode_short_u16<'de>(reader: &mut impl Reader<'de>) -> ReadResult<u16> {
    let mut val: u16 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = *reader.peek()?;
        reader.consume(1)?;
        val |= ((byte & 0x7F) as u16) << shift;
        if byte & 0x80 == 0 {
            return Ok(val);
        }
        shift += 7;
        if shift > 16 {
            return Err(ReadError::Custom("short_u16 overflow"));
        }
    }
}

/// Wrapper for u8 to implement Schema traits (orphan rules)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct U8Schema(pub u8);

impl SchemaWrite for U8Schema {
    type Src = U8Schema;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(1)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write(&[src.0])?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for U8Schema {
    type Dst = U8Schema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let byte = *reader.peek()?;
        reader.consume(1)?;
        dst.write(U8Schema(byte));
        Ok(())
    }
}

// ShortVec for u8 (bytes) - most common case, optimized
impl SchemaWrite for ShortVec<u8> {
    type Src = ShortVec<u8>;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        let len = src.0.len();
        if len > u16::MAX as usize {
            return Err(WriteError::Custom("ShortVec too long"));
        }
        Ok(short_u16_size(len as u16) + len)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        let len = src.0.len();
        if len > u16::MAX as usize {
            return Err(WriteError::Custom("ShortVec too long"));
        }
        encode_short_u16(writer, len as u16)?;
        writer.write(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for ShortVec<u8> {
    type Dst = ShortVec<u8>;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let len = decode_short_u16(reader)? as usize;
        let bytes = reader.fill_exact(len)?;
        let vec = bytes.to_vec();
        reader.consume(len)?;
        dst.write(ShortVec(vec));
        Ok(())
    }
}

macro_rules! impl_short_vec_schema {
    ($ty:ty) => {
        impl SchemaWrite for ShortVec<$ty> {
            type Src = ShortVec<$ty>;

            fn size_of(src: &Self::Src) -> WriteResult<usize> {
                let len = src.0.len();
                if len > u16::MAX as usize {
                    return Err(WriteError::Custom("ShortVec too long"));
                }
                let mut size = short_u16_size(len as u16);
                for item in &src.0 {
                    size += <$ty as SchemaWrite>::size_of(item)?;
                }
                Ok(size)
            }

            fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
                let len = src.0.len();
                if len > u16::MAX as usize {
                    return Err(WriteError::Custom("ShortVec too long"));
                }
                encode_short_u16(writer, len as u16)?;
                for item in &src.0 {
                    <$ty as SchemaWrite>::write(writer, item)?;
                }
                Ok(())
            }
        }

        impl<'de> SchemaRead<'de> for ShortVec<$ty> {
            type Dst = ShortVec<$ty>;

            fn read(
                reader: &mut impl Reader<'de>,
                dst: &mut MaybeUninit<Self::Dst>,
            ) -> ReadResult<()> {
                let len = decode_short_u16(reader)? as usize;
                let mut vec = Vec::with_capacity(len);
                for _ in 0..len {
                    let mut item = MaybeUninit::uninit();
                    <$ty as SchemaRead<'de>>::read(reader, &mut item)?;
                    vec.push(unsafe { item.assume_init() });
                }
                dst.write(ShortVec(vec));
                Ok(())
            }
        }
    };
}

/// Schema wrapper for `Pubkey` (32-byte public key)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct PubkeySchema(pub [u8; 32]);

impl From<Pubkey> for PubkeySchema {
    fn from(pk: Pubkey) -> Self {
        Self(pk.to_bytes())
    }
}

impl From<PubkeySchema> for Pubkey {
    fn from(schema: PubkeySchema) -> Self {
        Pubkey::from(schema.0)
    }
}

impl SchemaWrite for PubkeySchema {
    type Src = PubkeySchema;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(32)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for PubkeySchema {
    type Dst = PubkeySchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let bytes = reader.fill_exact(32)?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        reader.consume(32)?;
        dst.write(PubkeySchema(arr));
        Ok(())
    }
}

/// Schema wrapper for `Hash` (32-byte hash)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct HashSchema(pub [u8; 32]);

impl From<Hash> for HashSchema {
    fn from(h: Hash) -> Self {
        Self(h.to_bytes())
    }
}

impl From<HashSchema> for Hash {
    fn from(schema: HashSchema) -> Self {
        Hash::from(schema.0)
    }
}

impl SchemaWrite for HashSchema {
    type Src = HashSchema;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(32)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for HashSchema {
    type Dst = HashSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let bytes = reader.fill_exact(32)?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        reader.consume(32)?;
        dst.write(HashSchema(arr));
        Ok(())
    }
}

/// Schema wrapper for `Signature` (64-byte signature)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct SignatureSchema(pub [u8; 64]);

impl SignatureSchema {
    /// Serialize to bytes using wincode
    pub fn to_bytes(&self) -> Result<Vec<u8>, wincode::Error> {
        let size = Self::size_of(self)?;
        let mut buf = vec![0u8; size];
        Self::write(&mut buf.as_mut_slice(), self)?;
        Ok(buf)
    }
}

impl From<Signature> for SignatureSchema {
    fn from(sig: Signature) -> Self {
        Self(sig.into())
    }
}

impl From<SignatureSchema> for Signature {
    fn from(schema: SignatureSchema) -> Self {
        Signature::from(schema.0)
    }
}

impl SchemaWrite for SignatureSchema {
    type Src = SignatureSchema;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(64)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SignatureSchema {
    type Dst = SignatureSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let bytes = reader.fill_exact(64)?;
        let mut arr = [0u8; 64];
        arr.copy_from_slice(bytes);
        reader.consume(64)?;
        dst.write(SignatureSchema(arr));
        Ok(())
    }
}

/// Schema for MessageHeader (fixed 3-byte structure)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeaderSchema {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

impl From<&MessageHeader> for MessageHeaderSchema {
    fn from(h: &MessageHeader) -> Self {
        Self {
            num_required_signatures: h.num_required_signatures,
            num_readonly_signed_accounts: h.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: h.num_readonly_unsigned_accounts,
        }
    }
}

impl From<MessageHeaderSchema> for MessageHeader {
    fn from(schema: MessageHeaderSchema) -> Self {
        Self {
            num_required_signatures: schema.num_required_signatures,
            num_readonly_signed_accounts: schema.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: schema.num_readonly_unsigned_accounts,
        }
    }
}

impl SchemaWrite for MessageHeaderSchema {
    type Src = MessageHeaderSchema;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(3)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write(&[
            src.num_required_signatures,
            src.num_readonly_signed_accounts,
            src.num_readonly_unsigned_accounts,
        ])?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for MessageHeaderSchema {
    type Dst = MessageHeaderSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let bytes = reader.fill_exact(3)?;
        let header = MessageHeaderSchema {
            num_required_signatures: bytes[0],
            num_readonly_signed_accounts: bytes[1],
            num_readonly_unsigned_accounts: bytes[2],
        };
        reader.consume(3)?;
        dst.write(header);
        Ok(())
    }
}

/// Schema for `CompiledInstruction` with short_vec encoded fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledInstructionSchema {
    pub program_id_index: u8,
    pub accounts: ShortVec<u8>,
    pub data: ShortVec<u8>,
}

impl From<&CompiledInstruction> for CompiledInstructionSchema {
    fn from(instr: &CompiledInstruction) -> Self {
        Self {
            program_id_index: instr.program_id_index,
            accounts: ShortVec::new(instr.accounts.clone()),
            data: ShortVec::new(instr.data.clone()),
        }
    }
}

impl From<CompiledInstructionSchema> for CompiledInstruction {
    fn from(schema: CompiledInstructionSchema) -> Self {
        Self {
            program_id_index: schema.program_id_index,
            accounts: schema.accounts.into_inner(),
            data: schema.data.into_inner(),
        }
    }
}

impl SchemaWrite for CompiledInstructionSchema {
    type Src = CompiledInstructionSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(1 + ShortVec::<u8>::size_of(&src.accounts)? + ShortVec::<u8>::size_of(&src.data)?)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write(&[src.program_id_index])?;
        ShortVec::<u8>::write(writer, &src.accounts)?;
        ShortVec::<u8>::write(writer, &src.data)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for CompiledInstructionSchema {
    type Dst = CompiledInstructionSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let program_id_index = *reader.peek()?;
        reader.consume(1)?;

        let mut accounts = MaybeUninit::uninit();
        ShortVec::<u8>::read(reader, &mut accounts)?;

        let mut data = MaybeUninit::uninit();
        ShortVec::<u8>::read(reader, &mut data)?;

        dst.write(CompiledInstructionSchema {
            program_id_index,
            accounts: unsafe { accounts.assume_init() },
            data: unsafe { data.assume_init() },
        });
        Ok(())
    }
}

/// Schema for `MessageAddressTableLookup` with short_vec encoded index vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAddressTableLookupSchema {
    pub account_key: PubkeySchema,
    pub writable_indexes: ShortVec<u8>,
    pub readonly_indexes: ShortVec<u8>,
}

impl From<&MessageAddressTableLookup> for MessageAddressTableLookupSchema {
    fn from(lookup: &MessageAddressTableLookup) -> Self {
        Self {
            account_key: PubkeySchema::from(lookup.account_key),
            writable_indexes: ShortVec::new(lookup.writable_indexes.clone()),
            readonly_indexes: ShortVec::new(lookup.readonly_indexes.clone()),
        }
    }
}

impl From<MessageAddressTableLookupSchema> for MessageAddressTableLookup {
    fn from(schema: MessageAddressTableLookupSchema) -> Self {
        Self {
            account_key: schema.account_key.into(),
            writable_indexes: schema.writable_indexes.into_inner(),
            readonly_indexes: schema.readonly_indexes.into_inner(),
        }
    }
}

impl SchemaWrite for MessageAddressTableLookupSchema {
    type Src = MessageAddressTableLookupSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(PubkeySchema::size_of(&src.account_key)?
            + ShortVec::<u8>::size_of(&src.writable_indexes)?
            + ShortVec::<u8>::size_of(&src.readonly_indexes)?)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        PubkeySchema::write(writer, &src.account_key)?;
        ShortVec::<u8>::write(writer, &src.writable_indexes)?;
        ShortVec::<u8>::write(writer, &src.readonly_indexes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for MessageAddressTableLookupSchema {
    type Dst = MessageAddressTableLookupSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut account_key = MaybeUninit::uninit();
        PubkeySchema::read(reader, &mut account_key)?;

        let mut writable_indexes = MaybeUninit::uninit();
        ShortVec::<u8>::read(reader, &mut writable_indexes)?;

        let mut readonly_indexes = MaybeUninit::uninit();
        ShortVec::<u8>::read(reader, &mut readonly_indexes)?;

        dst.write(MessageAddressTableLookupSchema {
            account_key: unsafe { account_key.assume_init() },
            writable_indexes: unsafe { writable_indexes.assume_init() },
            readonly_indexes: unsafe { readonly_indexes.assume_init() },
        });
        Ok(())
    }
}

// Implement ShortVec for the schema types we need
impl_short_vec_schema!(PubkeySchema);
impl_short_vec_schema!(SignatureSchema);
impl_short_vec_schema!(CompiledInstructionSchema);
impl_short_vec_schema!(MessageAddressTableLookupSchema);

/// Schema for `legacy::Message` with short_vec encoded fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyMessageSchema {
    pub header: MessageHeaderSchema,
    pub account_keys: ShortVec<PubkeySchema>,
    pub recent_blockhash: HashSchema,
    pub instructions: ShortVec<CompiledInstructionSchema>,
}

impl From<&legacy::Message> for LegacyMessageSchema {
    fn from(msg: &legacy::Message) -> Self {
        Self {
            header: MessageHeaderSchema::from(&msg.header),
            account_keys: ShortVec::new(
                msg.account_keys
                    .iter()
                    .copied()
                    .map(PubkeySchema::from)
                    .collect(),
            ),
            recent_blockhash: HashSchema::from(msg.recent_blockhash),
            instructions: ShortVec::new(
                msg.instructions
                    .iter()
                    .map(CompiledInstructionSchema::from)
                    .collect(),
            ),
        }
    }
}

impl From<LegacyMessageSchema> for legacy::Message {
    fn from(schema: LegacyMessageSchema) -> Self {
        Self {
            header: schema.header.into(),
            account_keys: schema
                .account_keys
                .into_inner()
                .into_iter()
                .map(Pubkey::from)
                .collect(),
            recent_blockhash: schema.recent_blockhash.into(),
            instructions: schema
                .instructions
                .into_inner()
                .into_iter()
                .map(CompiledInstruction::from)
                .collect(),
        }
    }
}

impl SchemaWrite for LegacyMessageSchema {
    type Src = LegacyMessageSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(MessageHeaderSchema::size_of(&src.header)?
            + ShortVec::<PubkeySchema>::size_of(&src.account_keys)?
            + HashSchema::size_of(&src.recent_blockhash)?
            + ShortVec::<CompiledInstructionSchema>::size_of(&src.instructions)?)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        MessageHeaderSchema::write(writer, &src.header)?;
        ShortVec::<PubkeySchema>::write(writer, &src.account_keys)?;
        HashSchema::write(writer, &src.recent_blockhash)?;
        ShortVec::<CompiledInstructionSchema>::write(writer, &src.instructions)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for LegacyMessageSchema {
    type Dst = LegacyMessageSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut header = MaybeUninit::uninit();
        MessageHeaderSchema::read(reader, &mut header)?;

        let mut account_keys = MaybeUninit::uninit();
        ShortVec::<PubkeySchema>::read(reader, &mut account_keys)?;

        let mut recent_blockhash = MaybeUninit::uninit();
        HashSchema::read(reader, &mut recent_blockhash)?;

        let mut instructions = MaybeUninit::uninit();
        ShortVec::<CompiledInstructionSchema>::read(reader, &mut instructions)?;

        dst.write(LegacyMessageSchema {
            header: unsafe { header.assume_init() },
            account_keys: unsafe { account_keys.assume_init() },
            recent_blockhash: unsafe { recent_blockhash.assume_init() },
            instructions: unsafe { instructions.assume_init() },
        });
        Ok(())
    }
}

/// Schema for `v0::Message` with short_vec encoded fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V0MessageSchema {
    pub header: MessageHeaderSchema,
    pub account_keys: ShortVec<PubkeySchema>,
    pub recent_blockhash: HashSchema,
    pub instructions: ShortVec<CompiledInstructionSchema>,
    pub address_table_lookups: ShortVec<MessageAddressTableLookupSchema>,
}

impl From<&solana_message::v0::Message> for V0MessageSchema {
    fn from(msg: &solana_message::v0::Message) -> Self {
        Self {
            header: MessageHeaderSchema::from(&msg.header),
            account_keys: ShortVec::new(
                msg.account_keys
                    .iter()
                    .copied()
                    .map(PubkeySchema::from)
                    .collect(),
            ),
            recent_blockhash: HashSchema::from(msg.recent_blockhash),
            instructions: ShortVec::new(
                msg.instructions
                    .iter()
                    .map(CompiledInstructionSchema::from)
                    .collect(),
            ),
            address_table_lookups: ShortVec::new(
                msg.address_table_lookups
                    .iter()
                    .map(MessageAddressTableLookupSchema::from)
                    .collect(),
            ),
        }
    }
}

impl From<V0MessageSchema> for solana_message::v0::Message {
    fn from(schema: V0MessageSchema) -> Self {
        Self {
            header: schema.header.into(),
            account_keys: schema
                .account_keys
                .into_inner()
                .into_iter()
                .map(Pubkey::from)
                .collect(),
            recent_blockhash: schema.recent_blockhash.into(),
            instructions: schema
                .instructions
                .into_inner()
                .into_iter()
                .map(CompiledInstruction::from)
                .collect(),
            address_table_lookups: schema
                .address_table_lookups
                .into_inner()
                .into_iter()
                .map(MessageAddressTableLookup::from)
                .collect(),
        }
    }
}

impl SchemaWrite for V0MessageSchema {
    type Src = V0MessageSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(MessageHeaderSchema::size_of(&src.header)?
            + ShortVec::<PubkeySchema>::size_of(&src.account_keys)?
            + HashSchema::size_of(&src.recent_blockhash)?
            + ShortVec::<CompiledInstructionSchema>::size_of(&src.instructions)?
            + ShortVec::<MessageAddressTableLookupSchema>::size_of(&src.address_table_lookups)?)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        MessageHeaderSchema::write(writer, &src.header)?;
        ShortVec::<PubkeySchema>::write(writer, &src.account_keys)?;
        HashSchema::write(writer, &src.recent_blockhash)?;
        ShortVec::<CompiledInstructionSchema>::write(writer, &src.instructions)?;
        ShortVec::<MessageAddressTableLookupSchema>::write(writer, &src.address_table_lookups)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for V0MessageSchema {
    type Dst = V0MessageSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut header = MaybeUninit::uninit();
        MessageHeaderSchema::read(reader, &mut header)?;

        let mut account_keys = MaybeUninit::uninit();
        ShortVec::<PubkeySchema>::read(reader, &mut account_keys)?;

        let mut recent_blockhash = MaybeUninit::uninit();
        HashSchema::read(reader, &mut recent_blockhash)?;

        let mut instructions = MaybeUninit::uninit();
        ShortVec::<CompiledInstructionSchema>::read(reader, &mut instructions)?;

        let mut address_table_lookups = MaybeUninit::uninit();
        ShortVec::<MessageAddressTableLookupSchema>::read(reader, &mut address_table_lookups)?;

        dst.write(V0MessageSchema {
            header: unsafe { header.assume_init() },
            account_keys: unsafe { account_keys.assume_init() },
            recent_blockhash: unsafe { recent_blockhash.assume_init() },
            instructions: unsafe { instructions.assume_init() },
            address_table_lookups: unsafe { address_table_lookups.assume_init() },
        });
        Ok(())
    }
}

/// Schema for `VersionedMessage` with custom bit-masked wire format.
///
/// Wire format:
/// - Legacy: No prefix, first byte is `num_required_signatures` (always < 0x80)
/// - V0: 0x80 prefix byte, then V0 message bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedMessageSchema {
    Legacy(LegacyMessageSchema),
    V0(V0MessageSchema),
}

impl From<&VersionedMessage> for VersionedMessageSchema {
    fn from(msg: &VersionedMessage) -> Self {
        match msg {
            VersionedMessage::Legacy(legacy) => {
                VersionedMessageSchema::Legacy(LegacyMessageSchema::from(legacy))
            }
            VersionedMessage::V0(v0) => VersionedMessageSchema::V0(V0MessageSchema::from(v0)),
        }
    }
}

impl From<VersionedMessageSchema> for VersionedMessage {
    fn from(schema: VersionedMessageSchema) -> Self {
        match schema {
            VersionedMessageSchema::Legacy(legacy) => VersionedMessage::Legacy(legacy.into()),
            VersionedMessageSchema::V0(v0) => VersionedMessage::V0(v0.into()),
        }
    }
}

impl SchemaWrite for VersionedMessageSchema {
    type Src = VersionedMessageSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        match src {
            VersionedMessageSchema::Legacy(legacy) => LegacyMessageSchema::size_of(legacy),
            VersionedMessageSchema::V0(v0) => Ok(1 + V0MessageSchema::size_of(v0)?),
        }
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        match src {
            VersionedMessageSchema::Legacy(legacy) => {
                // Legacy: write directly, no prefix
                LegacyMessageSchema::write(writer, legacy)?;
            }
            VersionedMessageSchema::V0(v0) => {
                // V0: write 0x80 prefix, then message
                writer.write(&[0x80])?;
                V0MessageSchema::write(writer, v0)?;
            }
        }
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for VersionedMessageSchema {
    type Dst = VersionedMessageSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        // Peek first byte to determine variant
        let first_byte = *reader.peek()?;

        if first_byte & 0x80 == 0 {
            // Legacy: first byte is num_required_signatures, read full message
            let mut legacy = MaybeUninit::uninit();
            LegacyMessageSchema::read(reader, &mut legacy)?;
            dst.write(VersionedMessageSchema::Legacy(unsafe {
                legacy.assume_init()
            }));
        } else {
            // V0: first byte is 0x80 prefix
            if first_byte != 0x80 {
                return Err(ReadError::InvalidTagEncoding(first_byte as usize));
            }
            // Consume the prefix
            reader.consume(1)?;
            // Read V0 message
            let mut v0 = MaybeUninit::uninit();
            V0MessageSchema::read(reader, &mut v0)?;
            dst.write(VersionedMessageSchema::V0(unsafe { v0.assume_init() }));
        }
        Ok(())
    }
}

/// Schema for `VersionedTransaction` with short_vec encoded signatures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedTransactionSchema {
    pub signatures: ShortVec<SignatureSchema>,
    pub message: VersionedMessageSchema,
}

impl From<&VersionedTransaction> for VersionedTransactionSchema {
    fn from(tx: &VersionedTransaction) -> Self {
        Self {
            signatures: ShortVec::new(
                tx.signatures
                    .iter()
                    .copied()
                    .map(SignatureSchema::from)
                    .collect(),
            ),
            message: VersionedMessageSchema::from(&tx.message),
        }
    }
}

impl From<VersionedTransactionSchema> for VersionedTransaction {
    fn from(schema: VersionedTransactionSchema) -> Self {
        Self {
            signatures: schema
                .signatures
                .into_inner()
                .into_iter()
                .map(Signature::from)
                .collect(),
            message: schema.message.into(),
        }
    }
}

impl SchemaWrite for VersionedTransactionSchema {
    type Src = VersionedTransactionSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(ShortVec::<SignatureSchema>::size_of(&src.signatures)?
            + VersionedMessageSchema::size_of(&src.message)?)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        ShortVec::<SignatureSchema>::write(writer, &src.signatures)?;
        VersionedMessageSchema::write(writer, &src.message)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for VersionedTransactionSchema {
    type Dst = VersionedTransactionSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut signatures = MaybeUninit::uninit();
        ShortVec::<SignatureSchema>::read(reader, &mut signatures)?;

        let mut message = MaybeUninit::uninit();
        VersionedMessageSchema::read(reader, &mut message)?;

        dst.write(VersionedTransactionSchema {
            signatures: unsafe { signatures.assume_init() },
            message: unsafe { message.assume_init() },
        });
        Ok(())
    }
}

/// Serialize a `VersionedTransaction` using wincode with Solana wire format.
pub fn serialize_transaction(tx: &VersionedTransaction) -> Result<Vec<u8>, wincode::Error> {
    let schema = VersionedTransactionSchema::from(tx);
    let size = VersionedTransactionSchema::size_of(&schema)?;
    let mut buf = vec![0u8; size];
    VersionedTransactionSchema::write(&mut buf.as_mut_slice(), &schema)?;
    Ok(buf)
}

/// Deserialize a `VersionedTransaction` from bytes using wincode.
/// This allows trailing bytes (matching bincode's `allow_trailing_bytes`).
pub fn deserialize_transaction(bytes: &[u8]) -> Result<VersionedTransaction, wincode::Error> {
    let mut cursor = Cursor::new(bytes);
    let mut dst = MaybeUninit::uninit();
    VersionedTransactionSchema::read(&mut cursor, &mut dst)?;
    let schema = unsafe { dst.assume_init() };
    Ok(schema.into())
}

/// Deserialize a `VersionedTransaction` with a size limit, allowing trailing bytes.
/// Returns the transaction and the number of bytes consumed.
pub fn deserialize_transaction_with_limit(
    bytes: &[u8],
    limit: usize,
) -> Result<(VersionedTransaction, usize), wincode::Error> {
    if bytes.len() > limit {
        return Err(wincode::Error::ReadError(
            ReadError::PreallocationSizeLimit {
                needed: bytes.len(),
                limit,
            },
        ));
    }
    let mut cursor = Cursor::new(bytes);
    let mut dst = MaybeUninit::uninit();
    VersionedTransactionSchema::read(&mut cursor, &mut dst)?;
    let schema = unsafe { dst.assume_init() };
    let consumed = cursor.position();
    Ok((schema.into(), consumed))
}

/// Schema for SigChallenge with u64 length prefix encoding (matching bincode default).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigChallengeSchema {
    pub pubkey_to_verify: PubkeySchema,
    pub timestamp: u64,
    pub nonce: Vec<u8>,
}

impl SigChallengeSchema {
    /// Serialize to bytes using wincode
    pub fn to_bytes(&self) -> Result<Vec<u8>, wincode::Error> {
        let size = Self::size_of(self)?;
        let mut buf = vec![0u8; size];
        Self::write(&mut buf.as_mut_slice(), self)?;
        Ok(buf)
    }
}

impl SchemaWrite for SigChallengeSchema {
    type Src = SigChallengeSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(32 + 8 + 8 + src.nonce.len())
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        PubkeySchema::write(writer, &src.pubkey_to_verify)?;
        writer.write(&src.timestamp.to_le_bytes())?;
        // Standard bincode Vec encoding: u64 length prefix (little-endian)
        writer.write(&(src.nonce.len() as u64).to_le_bytes())?;
        writer.write(&src.nonce)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SigChallengeSchema {
    type Dst = SigChallengeSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut pubkey = MaybeUninit::uninit();
        PubkeySchema::read(reader, &mut pubkey)?;

        let timestamp_bytes = reader.fill_exact(8)?;
        let timestamp = u64::from_le_bytes(timestamp_bytes.try_into().unwrap());
        reader.consume(8)?;

        let len_bytes = reader.fill_exact(8)?;
        let len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        reader.consume(8)?;

        let nonce_bytes = reader.fill_exact(len)?;
        let nonce = nonce_bytes.to_vec();
        reader.consume(len)?;

        dst.write(SigChallengeSchema {
            pubkey_to_verify: unsafe { pubkey.assume_init() },
            timestamp,
            nonce,
        });
        Ok(())
    }
}

/// Schema for SignedSigChallenge: (Signature, Vec<u8>)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedSigChallengeSchema {
    pub signature: SignatureSchema,
    pub payload: Vec<u8>,
}

impl SignedSigChallengeSchema {
    /// Serialize to bytes using wincode
    pub fn to_bytes(&self) -> Result<Vec<u8>, wincode::Error> {
        let size = Self::size_of(self)?;
        let mut buf = vec![0u8; size];
        Self::write(&mut buf.as_mut_slice(), self)?;
        Ok(buf)
    }
}

impl SchemaWrite for SignedSigChallengeSchema {
    type Src = SignedSigChallengeSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(64 + 8 + src.payload.len())
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        SignatureSchema::write(writer, &src.signature)?;
        writer.write(&(src.payload.len() as u64).to_le_bytes())?;
        writer.write(&src.payload)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SignedSigChallengeSchema {
    type Dst = SignedSigChallengeSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut signature = MaybeUninit::uninit();
        SignatureSchema::read(reader, &mut signature)?;

        let len_bytes = reader.fill_exact(8)?;
        let len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        reader.consume(8)?;

        let payload_bytes = reader.fill_exact(len)?;
        let payload = payload_bytes.to_vec();
        reader.consume(len)?;

        dst.write(SignedSigChallengeSchema {
            signature: unsafe { signature.assume_init() },
            payload,
        });
        Ok(())
    }
}

/// Schema for OneTimeAuthToken
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneTimeAuthTokenSchema {
    pub pubkey: PubkeySchema,
    pub nonce: Vec<u8>,
}

impl OneTimeAuthTokenSchema {
    /// Serialize to bytes using wincode
    pub fn to_bytes(&self) -> Result<Vec<u8>, wincode::Error> {
        let size = Self::size_of(self)?;
        let mut buf = vec![0u8; size];
        Self::write(&mut buf.as_mut_slice(), self)?;
        Ok(buf)
    }

    /// Deserialize from bytes using wincode
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, wincode::Error> {
        let mut cursor = Cursor::new(bytes);
        let mut dst = MaybeUninit::uninit();
        Self::read(&mut cursor, &mut dst)?;
        Ok(unsafe { dst.assume_init() })
    }
}

impl SchemaWrite for OneTimeAuthTokenSchema {
    type Src = OneTimeAuthTokenSchema;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(32 + 8 + src.nonce.len())
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        PubkeySchema::write(writer, &src.pubkey)?;
        writer.write(&(src.nonce.len() as u64).to_le_bytes())?;
        writer.write(&src.nonce)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for OneTimeAuthTokenSchema {
    type Dst = OneTimeAuthTokenSchema;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut pubkey = MaybeUninit::uninit();
        PubkeySchema::read(reader, &mut pubkey)?;

        let len_bytes = reader.fill_exact(8)?;
        let len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        reader.consume(8)?;

        let nonce_bytes = reader.fill_exact(len)?;
        let nonce = nonce_bytes.to_vec();
        reader.consume(len)?;

        dst.write(OneTimeAuthTokenSchema {
            pubkey: unsafe { pubkey.assume_init() },
            nonce,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_u16_encoding() {
        // Test compact-u16 encoding roundtrip
        let test_cases: Vec<(u16, usize)> = vec![
            (0, 1),
            (1, 1),
            (127, 1),
            (128, 2),
            (255, 2),
            (16383, 2),
            (16384, 3),
            (65535, 3),
        ];

        for (val, expected_size) in test_cases {
            assert_eq!(short_u16_size(val), expected_size, "size of {val}");

            let mut buf = vec![0u8; expected_size];
            encode_short_u16(&mut buf.as_mut_slice(), val).unwrap();

            let mut cursor = Cursor::new(&buf[..]);
            let decoded = decode_short_u16(&mut cursor).unwrap();
            assert_eq!(decoded, val, "decoding {val}");
        }
    }

    #[test]
    fn test_pubkey_schema_roundtrip() {
        let pk = Pubkey::new_unique();
        let schema = PubkeySchema::from(pk);

        let size = PubkeySchema::size_of(&schema).unwrap();
        assert_eq!(size, 32);

        let mut buf = vec![0u8; size];
        PubkeySchema::write(&mut buf.as_mut_slice(), &schema).unwrap();

        let mut cursor = Cursor::new(&buf[..]);
        let mut decoded = MaybeUninit::uninit();
        PubkeySchema::read(&mut cursor, &mut decoded).unwrap();
        let decoded = unsafe { decoded.assume_init() };

        assert_eq!(schema, decoded);
        assert_eq!(Pubkey::from(decoded), pk);
    }

    #[test]
    fn test_message_header_schema_roundtrip() {
        let header = MessageHeaderSchema {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 3,
        };

        let size = MessageHeaderSchema::size_of(&header).unwrap();
        assert_eq!(size, 3);

        let mut buf = vec![0u8; size];
        MessageHeaderSchema::write(&mut buf.as_mut_slice(), &header).unwrap();

        let mut cursor = Cursor::new(&buf[..]);
        let mut decoded = MaybeUninit::uninit();
        MessageHeaderSchema::read(&mut cursor, &mut decoded).unwrap();
        let decoded = unsafe { decoded.assume_init() };

        assert_eq!(header, decoded);
    }
}
