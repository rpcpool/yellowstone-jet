use {
    solana_bincode::limited_deserialize,
    solana_packet::PACKET_DATA_SIZE,
    solana_pubkey::{ParsePubkeyError, Pubkey},
    solana_system_interface::instruction::SystemInstruction,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        collections::HashSet,
        path::{Path, PathBuf},
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum TipAccountsError {
    #[error("failed to read tipping address file {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid tipping address {value:?} on line {line}")]
    InvalidAddress {
        line: usize,
        value: String,
        #[source]
        source: ParsePubkeyError,
    },

    #[error("tipping address file contains no address")]
    Empty,
}

pub type Result<T> = std::result::Result<T, TipAccountsError>;

/// The set of addresses a transaction may tip to be accepted by the `/send-via-tip` endpoint.
///
/// A tip is a top-level System Program `Transfer` of at least one lamport whose destination
/// is one of these addresses.
///
/// # Examples
///
/// ```ignore
/// let tip_accounts = TipAccounts::load(Path::new("tipping-addresses.txt"))?;
/// if let Some(lamports) = tip_accounts.find_tip(&transaction, 1_000_000) {
///     // forward the transaction
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TipAccounts {
    accounts: HashSet<Pubkey>,
}

impl TipAccounts {
    /// Loads the tipping addresses from a text file, see [`TipAccounts::parse`] for the format.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the tipping address file.
    ///
    /// # Returns
    ///
    /// The [`TipAccounts`] listed in the file.
    ///
    /// # Errors
    ///
    /// Returns [`TipAccountsError::Read`] if the file cannot be read, and any error of
    /// [`TipAccounts::parse`] if its content is invalid.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|source| TipAccountsError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&content)
    }

    /// Parses one base58 address per line. Surrounding whitespace is trimmed, and blank lines
    /// and lines starting with `#` are ignored.
    ///
    /// # Arguments
    ///
    /// * `content` - The content of a tipping address file.
    ///
    /// # Returns
    ///
    /// The [`TipAccounts`] listed in `content`, without duplicates.
    ///
    /// # Errors
    ///
    /// Returns [`TipAccountsError::InvalidAddress`] if a line is not a valid address, and
    /// [`TipAccountsError::Empty`] if `content` lists no address.
    pub fn parse(content: &str) -> Result<Self> {
        let mut accounts = HashSet::new();
        for (index, line) in content.lines().enumerate() {
            let value = line.trim();
            if value.is_empty() || value.starts_with('#') {
                continue;
            }
            let account =
                Pubkey::from_str(value).map_err(|source| TipAccountsError::InvalidAddress {
                    line: index + 1,
                    value: value.to_owned(),
                    source,
                })?;
            accounts.insert(account);
        }
        if accounts.is_empty() {
            return Err(TipAccountsError::Empty);
        }
        Ok(Self { accounts })
    }

    /// Returns the number of distinct tipping addresses.
    ///
    /// # Returns
    ///
    /// The number of distinct tipping addresses, never zero.
    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    /// Returns whether there is no tipping address.
    ///
    /// # Returns
    ///
    /// `false` in practice: [`TipAccounts::parse`] rejects a file without any address.
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    /// Finds the first top-level instruction of `transaction` that tips one of these addresses.
    ///
    /// Only static account keys are considered: a destination loaded from an address lookup
    /// table cannot be resolved without fetching the table, so it does not count as a tip.
    /// Tips made through a CPI are not visible in the message and do not count either.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The [`VersionedTransaction`] to inspect. It does not need to be
    ///   sanitized: out-of-range indexes are treated as "not a tip".
    /// * `min_lamports` - The smallest transfer that counts as a tip. A transfer of zero
    ///   lamports never counts, even when `min_lamports` is zero.
    ///
    /// # Returns
    ///
    /// [`Some`] with the tipped lamports, or [`None`] if no instruction is a System Program
    /// `Transfer` of at least `min_lamports` (and at least one) lamports to a tipping address.
    pub fn find_tip(&self, transaction: &VersionedTransaction, min_lamports: u64) -> Option<u64> {
        let min_lamports = min_lamports.max(1);
        let account_keys = transaction.message.static_account_keys();
        transaction.message.instructions().iter().find_map(|ix| {
            let program_id = account_keys.get(usize::from(ix.program_id_index))?;
            if !solana_sdk_ids::system_program::check_id(program_id) {
                return None;
            }
            // Same limit the System Program uses to decode its instructions.
            let Ok(SystemInstruction::Transfer { lamports }) =
                limited_deserialize::<SystemInstruction>(&ix.data, PACKET_DATA_SIZE as u64)
            else {
                return None;
            };
            let destination = account_keys.get(usize::from(*ix.accounts.get(1)?))?;
            (lamports >= min_lamports && self.accounts.contains(destination)).then_some(lamports)
        })
    }
}

/// The smallest tip, in lamports, the `/send-via-tip` endpoint accepts.
///
/// It is shared between the tip endpoint, which reads it on every transaction, and the admin
/// API, which can change it at runtime. The value is independent of any other state, so
/// [`Ordering::Relaxed`] is enough and reading it costs no more than reading a plain field.
///
/// # Examples
///
/// ```ignore
/// let min_tip_lamports = Arc::new(MinTipLamports::new(10_000));
/// let previous = min_tip_lamports.set(1_000_000);
/// assert_eq!(tip_accounts.find_tip(&transaction, min_tip_lamports.get()), Some(1_000_000));
/// ```
#[derive(Debug, Default)]
pub struct MinTipLamports(AtomicU64);

impl MinTipLamports {
    /// Creates the shared minimum tip.
    ///
    /// # Arguments
    ///
    /// * `lamports` - The initial minimum tip, in lamports.
    ///
    /// # Returns
    ///
    /// A new `MinTipLamports` holding `lamports`.
    pub const fn new(lamports: u64) -> Self {
        Self(AtomicU64::new(lamports))
    }

    /// Returns the current minimum tip.
    ///
    /// # Returns
    ///
    /// The current minimum tip, in lamports.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Replaces the minimum tip. Transactions checked after this call use the new value.
    ///
    /// # Arguments
    ///
    /// * `lamports` - The new minimum tip, in lamports.
    ///
    /// # Returns
    ///
    /// The previous minimum tip, in lamports.
    pub fn set(&self, lamports: u64) -> u64 {
        self.0.swap(lamports, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_hash::Hash,
        solana_instruction::Instruction,
        solana_keypair::Keypair,
        solana_message::{AddressLookupTableAccount, VersionedMessage, v0},
        solana_signer::Signer,
        solana_system_interface::instruction::{transfer, transfer_with_seed},
    };

    fn tip_accounts(accounts: &[Pubkey]) -> TipAccounts {
        TipAccounts {
            accounts: accounts.iter().copied().collect(),
        }
    }

    fn tx_with_lookup_tables(
        payer: &Keypair,
        instructions: &[Instruction],
        lookup_tables: &[AddressLookupTableAccount],
    ) -> VersionedTransaction {
        VersionedTransaction::try_new(
            VersionedMessage::V0(
                v0::Message::try_compile(
                    &payer.pubkey(),
                    instructions,
                    lookup_tables,
                    Hash::new_unique(),
                )
                .expect("try compile"),
            ),
            &[payer],
        )
        .expect("try new")
    }

    fn tx(payer: &Keypair, instructions: &[Instruction]) -> VersionedTransaction {
        tx_with_lookup_tables(payer, instructions, &[])
    }

    #[test]
    fn min_tip_lamports_set_returns_previous_value() {
        let min_tip_lamports = MinTipLamports::new(10_000);

        assert_eq!(min_tip_lamports.set(1_000_000), 10_000);
        assert_eq!(min_tip_lamports.get(), 1_000_000);
    }

    #[test]
    fn parse_skips_blank_lines_comments_and_duplicates() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let content = format!("# tip accounts\n\n  {a}  \n{b}\n{a}\n");

        let parsed = TipAccounts::parse(&content).expect("parse");

        assert_eq!(parsed, tip_accounts(&[a, b]));
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn parse_rejects_invalid_address_with_line_number() {
        let content = format!("{}\nnot-a-pubkey\n", Pubkey::new_unique());

        let err = TipAccounts::parse(&content).expect_err("invalid address");

        assert!(matches!(
            err,
            TipAccountsError::InvalidAddress { line: 2, ref value, .. } if value == "not-a-pubkey"
        ));
    }

    #[test]
    fn parse_rejects_file_without_address() {
        assert!(matches!(
            TipAccounts::parse("# nothing\n\n"),
            Err(TipAccountsError::Empty)
        ));
    }

    #[test]
    fn load_reports_missing_file() {
        let path = Path::new("/nonexistent/tipping-addresses.txt");

        assert!(matches!(
            TipAccounts::load(path),
            Err(TipAccountsError::Read { .. })
        ));
    }

    #[test]
    fn find_tip_accepts_transfer_to_tip_account() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let tx = tx(
            &payer,
            &[
                transfer(&payer.pubkey(), &other, 5),
                transfer(&payer.pubkey(), &tip, 1_000_000),
            ],
        );

        assert_eq!(tip_accounts(&[tip]).find_tip(&tx, 1), Some(1_000_000));
    }

    #[test]
    fn find_tip_rejects_transfer_to_other_account() {
        let payer = Keypair::new();
        let tx = tx(
            &payer,
            &[transfer(&payer.pubkey(), &Pubkey::new_unique(), 1_000_000)],
        );

        assert_eq!(tip_accounts(&[Pubkey::new_unique()]).find_tip(&tx, 1), None);
    }

    #[test]
    fn find_tip_rejects_zero_lamports() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let tx = tx(&payer, &[transfer(&payer.pubkey(), &tip, 0)]);

        assert_eq!(tip_accounts(&[tip]).find_tip(&tx, 1), None);
        assert_eq!(tip_accounts(&[tip]).find_tip(&tx, 0), None);
    }

    #[test]
    fn find_tip_enforces_min_lamports() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let tx = tx(&payer, &[transfer(&payer.pubkey(), &tip, 1_000_000)]);
        let tip_accounts = tip_accounts(&[tip]);

        assert_eq!(tip_accounts.find_tip(&tx, 1_000_000), Some(1_000_000));
        assert_eq!(tip_accounts.find_tip(&tx, 1_000_001), None);
    }

    #[test]
    fn find_tip_skips_small_tip_for_a_later_large_one() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let tx = tx(
            &payer,
            &[
                transfer(&payer.pubkey(), &tip, 10),
                transfer(&payer.pubkey(), &tip, 2_000_000),
            ],
        );

        assert_eq!(
            tip_accounts(&[tip]).find_tip(&tx, 1_000_000),
            Some(2_000_000)
        );
    }

    #[test]
    fn find_tip_rejects_transfer_with_seed() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let from = Pubkey::create_with_seed(&payer.pubkey(), "seed", &owner).expect("seed");
        let tx = tx(
            &payer,
            &[transfer_with_seed(
                &from,
                &payer.pubkey(),
                "seed".to_owned(),
                &owner,
                &tip,
                1_000_000,
            )],
        );

        assert_eq!(tip_accounts(&[tip]).find_tip(&tx, 1), None);
    }

    #[test]
    fn find_tip_rejects_transfer_data_on_other_program() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let mut ix = transfer(&payer.pubkey(), &tip, 1_000_000);
        ix.program_id = Pubkey::new_unique();
        let tx = tx(&payer, &[ix]);

        assert_eq!(tip_accounts(&[tip]).find_tip(&tx, 1), None);
    }

    #[test]
    fn find_tip_rejects_tip_account_from_lookup_table() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let lookup_table = AddressLookupTableAccount {
            key: Pubkey::new_unique(),
            addresses: vec![tip],
        };
        let tx = tx_with_lookup_tables(
            &payer,
            &[transfer(&payer.pubkey(), &tip, 1_000_000)],
            &[lookup_table],
        );
        assert!(!tx.message.static_account_keys().contains(&tip));

        assert_eq!(tip_accounts(&[tip]).find_tip(&tx, 1), None);
    }
}
