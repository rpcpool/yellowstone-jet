use {
    core::fmt,
    serde::{Deserialize, Serialize},
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, SystemTime},
    },
    tokio::{sync::Mutex, time::Instant},
    uuid::Uuid,
};

#[derive(Debug, thiserror::Error)]
pub enum VerifyAnswerError {
    InvalidChallengeFormat,
    InvalidChallenge,
    InvalidChallengeeSignature,
    ChallengeExpired,
}

impl fmt::Display for VerifyAnswerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VerifyAnswerError::InvalidChallengeFormat => write!(f, "Invalid challenge format"),
            VerifyAnswerError::InvalidChallenge => write!(f, "Invalid answer"),
            VerifyAnswerError::InvalidChallengeeSignature => {
                write!(f, "Invalid challengee signature")
            }
            VerifyAnswerError::ChallengeExpired => write!(f, "Challenge expired"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OneTimeAuthTokenClaimError {
    NotFound,
    PubkeyMismatch,
}

impl fmt::Display for OneTimeAuthTokenClaimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OneTimeAuthTokenClaimError::NotFound => write!(f, "Not found"),
            OneTimeAuthTokenClaimError::PubkeyMismatch => write!(f, "Pubkey mismatch"),
        }
    }
}

#[async_trait::async_trait]
pub trait OneTimeAuthTokenStore {
    async fn generate_new(&self, pubkey: Pubkey, ttl: Duration) -> OneTimeAuthToken;
    async fn claim(&self, token: OneTimeAuthToken) -> Result<(), OneTimeAuthTokenClaimError>;
}

#[async_trait::async_trait]
pub trait PubkeyChallenger {
    async fn create_challenge(&self, pubkey_to_verify: Pubkey) -> String;

    async fn verify_answer(
        &self,
        pubkey_to_verify: Pubkey,
        challenge: String,
        challengee_signature: Signature,
        challengee_nonce: Vec<u8>,
    ) -> Result<OneTimeAuthToken, VerifyAnswerError>;

    async fn use_one_time_auth_token(&self, token: OneTimeAuthToken) -> bool;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SigChallenge {
    pubkey_to_verify: Pubkey,
    timestamp: u64,
    nonce: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SignedSigChallenge(Signature, Vec<u8>);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct OneTimeAuthToken {
    pubkey: Pubkey,
    nonce: Vec<u8>,
}

impl OneTimeAuthToken {
    pub const fn pubkey(&self) -> Pubkey {
        self.pubkey
    }
}

/// A challenger that uses a signature to create/verify challenges
pub struct SigChallenger<S, TS> {
    challenge_signer: S,
    token_store: TS,
    one_time_auth_token_ttl: Duration,
}

impl<S, TS> SigChallenger<S, TS>
where
    S: Signer,
    TS: OneTimeAuthTokenStore,
{
    pub const fn new(
        challenge_signer: S,
        token_store: TS,
        one_time_auth_token_ttl: Duration,
    ) -> Self {
        Self {
            challenge_signer,
            token_store,
            one_time_auth_token_ttl,
        }
    }
}

pub fn default_sig_challenger() -> SigChallenger<Keypair, InMemoryTokenStore> {
    let keypair = Keypair::new();
    let token_store = InMemoryTokenStore::default();
    SigChallenger::new(keypair, token_store, Duration::from_secs(300))
}

pub fn append_nonce_and_sign(
    signer: &dyn Signer,
    challenge: impl AsRef<[u8]>,
    nonce: impl AsRef<[u8]>,
) -> Signature {
    let message = [challenge.as_ref(), nonce.as_ref()].concat();
    signer.sign_message(&message)
}

#[async_trait::async_trait]
impl<S, TS> PubkeyChallenger for SigChallenger<S, TS>
where
    S: Signer + Send + Sync + 'static,
    TS: OneTimeAuthTokenStore + Send + Sync + 'static,
{
    async fn create_challenge(&self, pubkey_to_verify: Pubkey) -> String {
        let nonce = Uuid::new_v4().into_bytes().to_vec();
        let since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("fail to get unix epoch")
            .as_secs();
        let challenge = SigChallenge {
            pubkey_to_verify,
            timestamp: since_epoch,
            nonce,
        };
        let ser_challenge = bincode::serialize(&challenge).expect("serialize challenge");

        let signature = self.challenge_signer.sign_message(&ser_challenge);

        let sig_challenge = SignedSigChallenge(signature, ser_challenge);
        let sig_challenge = bincode::serialize(&sig_challenge).expect("serialize signed challenge");
        bs58::encode(sig_challenge).into_string()
    }

    async fn verify_answer(
        &self,
        pubkey_to_verify: Pubkey,
        challenge: String,
        challengee_signature: Signature,
        challengee_nonce: Vec<u8>,
    ) -> Result<OneTimeAuthToken, VerifyAnswerError> {
        let bincoded_challenge = bs58::decode(&challenge)
            .into_vec()
            .map_err(|_| VerifyAnswerError::InvalidChallengeFormat)?;

        let sig_challenge: SignedSigChallenge = bincode::deserialize(&bincoded_challenge)
            .map_err(|_| VerifyAnswerError::InvalidChallengeFormat)?;

        let sig_challenge_payload = bincode::deserialize::<SigChallenge>(&sig_challenge.1)
            .map_err(|_| VerifyAnswerError::InvalidChallengeFormat)?;

        // Make sure the challenge was issued by us
        let is_legit = sig_challenge.0.verify(
            &self.challenge_signer.pubkey().to_bytes(),
            sig_challenge.1.as_ref(),
        );

        if !is_legit {
            return Err(VerifyAnswerError::InvalidChallenge);
        }

        let challenge_created_at =
            SystemTime::UNIX_EPOCH + Duration::from_secs(sig_challenge_payload.timestamp);
        if SystemTime::now()
            .duration_since(challenge_created_at)
            .expect("time")
            > self.one_time_auth_token_ttl
        {
            return Err(VerifyAnswerError::ChallengeExpired);
        }

        // Verify `Pubkey_client.verify(challenge || challengee_nonce)`
        let expected_challengee_message = [challenge.as_bytes(), &challengee_nonce].concat();
        let is_legit = challengee_signature.verify(
            &sig_challenge_payload.pubkey_to_verify.to_bytes(),
            &expected_challengee_message,
        );

        // Now make sure the challengee's signature is valid
        if !is_legit {
            return Err(VerifyAnswerError::InvalidChallengeeSignature);
        }
        let token = self
            .token_store
            .generate_new(pubkey_to_verify, self.one_time_auth_token_ttl)
            .await;
        Ok(token)
    }

    async fn use_one_time_auth_token(&self, token: OneTimeAuthToken) -> bool {
        match self.token_store.claim(token).await {
            Ok(()) => true,
            Err(_) => false,
        }
    }
}

pub struct InMemoryTokenStore {
    inner: Arc<Mutex<HashMap<OneTimeAuthToken, (Pubkey, Instant)>>>,
}

impl Default for InMemoryTokenStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl OneTimeAuthTokenStore for InMemoryTokenStore {
    async fn generate_new(&self, pubkey: Pubkey, ttl: Duration) -> OneTimeAuthToken {
        let token = OneTimeAuthToken {
            pubkey,
            nonce: Uuid::new_v4().as_bytes().to_vec(),
        };
        let expire_at = Instant::now() + ttl;
        let mut inner = self.inner.lock().await;
        if let Some(old) = inner.insert(token.clone(), (pubkey, expire_at)) {
            panic!("Token collision: with {:?}", old);
        }

        // Here we garbage collect expired tokens
        inner.retain(|_, (_, deadline)| deadline.elapsed() == Duration::ZERO);
        token
    }

    async fn claim(&self, token: OneTimeAuthToken) -> Result<(), OneTimeAuthTokenClaimError> {
        let mut inner = self.inner.lock().await;
        if let Some((actual_bound_pubkey, deadline)) = inner.remove(&token) {
            if deadline < Instant::now() {
                return Err(OneTimeAuthTokenClaimError::NotFound);
            }
            if actual_bound_pubkey != token.pubkey {
                return Err(OneTimeAuthTokenClaimError::PubkeyMismatch);
            }
            // Here we garbage collect expired tokens
            inner.retain(|_, (_, deadline)| deadline.elapsed() == Duration::ZERO);
            Ok(())
        } else {
            Err(OneTimeAuthTokenClaimError::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::signature::{Keypair, Signer},
        std::time::Duration,
    };

    #[tokio::test]
    async fn test_sig_challenger() {
        let challenger_keypair = Keypair::new();
        let challengee_keypair = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let challenger =
            SigChallenger::new(challenger_keypair, token_store, Duration::from_secs(60));

        let pubkey_to_verify = challengee_keypair.pubkey();

        let challenge = challenger.create_challenge(pubkey_to_verify).await;
        let nonce = vec![0u8; 64];

        let sig = append_nonce_and_sign(&challengee_keypair, &challenge, &nonce);

        let token = challenger
            .verify_answer(pubkey_to_verify, challenge, sig, nonce)
            .await
            .expect("verify answer");
        assert!(challenger.use_one_time_auth_token(token).await);
    }

    #[tokio::test]
    async fn sig_challenger_should_generate_random_challenge_for_same_input() {
        let challenger_keypair = Keypair::new();
        let challengee_keypair = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let challenger =
            SigChallenger::new(challenger_keypair, token_store, Duration::from_secs(60));
        let pubkey_to_verify = challengee_keypair.pubkey();
        let challenge1 = challenger.create_challenge(pubkey_to_verify).await;
        let challenge2 = challenger.create_challenge(pubkey_to_verify).await;
        assert_ne!(challenge1, challenge2);
    }

    #[tokio::test]
    async fn sig_challenger_should_refuse_forged_challenge() {
        let challenger_keypair = Keypair::new();
        let challengee_keypair = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let challenger =
            SigChallenger::new(challenger_keypair, token_store, Duration::from_secs(60));
        let forger = SigChallenger::new(
            Keypair::new(),
            InMemoryTokenStore::default(),
            Duration::from_secs(60),
        );
        let pubkey_to_verify = challengee_keypair.pubkey();
        let forged_challenge = forger.create_challenge(challengee_keypair.pubkey()).await;
        let nonce = vec![0u8; 64];
        let challengee_sig = append_nonce_and_sign(&challengee_keypair, &forged_challenge, &nonce);
        let token = challenger
            .verify_answer(pubkey_to_verify, forged_challenge, challengee_sig, nonce)
            .await;
        assert!(matches!(token, Err(VerifyAnswerError::InvalidChallenge)));
    }

    #[tokio::test]
    async fn sig_challenger_should_refuse_malformated_challenge() {
        let challenger_keypair = Keypair::new();
        let challengee_keypair = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let challenger =
            SigChallenger::new(challenger_keypair, token_store, Duration::from_secs(60));
        let pubkey_to_verify = challengee_keypair.pubkey();
        let forged_challenge = "bad".to_string();
        let nonce = vec![0u8; 64];
        let challengee_sig = append_nonce_and_sign(&challengee_keypair, &forged_challenge, &nonce);
        let token = challenger
            .verify_answer(pubkey_to_verify, forged_challenge, challengee_sig, nonce)
            .await;
        assert!(matches!(
            token,
            Err(VerifyAnswerError::InvalidChallengeFormat)
        ));
    }

    #[tokio::test]
    async fn sig_challenger_token_claim_should_fail_if_forged() {
        let challenger_keypair = Keypair::new();
        let challengee_keypair = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let challenger =
            SigChallenger::new(challenger_keypair, token_store, Duration::from_secs(60));

        let token = OneTimeAuthToken {
            pubkey: challengee_keypair.pubkey(),
            nonce: vec![0u8; 64],
        };
        assert!(!challenger.use_one_time_auth_token(token).await);
    }

    #[tokio::test]
    async fn sig_challenger_should_refuse_expired_challenge() {
        let challenger_keypair = Keypair::new();
        let challengee_keypair = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let challenger =
            SigChallenger::new(challenger_keypair, token_store, Duration::from_secs(0));
        let pubkey_to_verify = challengee_keypair.pubkey();
        let challenge = challenger.create_challenge(pubkey_to_verify).await;
        let nonce = vec![0u8; 64];
        let challengee_sig = append_nonce_and_sign(&challengee_keypair, &challenge, &nonce);
        let token = challenger
            .verify_answer(pubkey_to_verify, challenge, challengee_sig, nonce)
            .await;
        assert!(matches!(token, Err(VerifyAnswerError::ChallengeExpired)));
    }

    #[tokio::test]
    async fn in_memory_token_store_should_not_claim_expired_token() {
        let keyapir = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let token = token_store
            .generate_new(keyapir.pubkey(), Duration::from_secs(0))
            .await;
        assert!(matches!(
            token_store.claim(token).await,
            Err(OneTimeAuthTokenClaimError::NotFound)
        ));
    }

    #[tokio::test]
    async fn in_memory_token_store_should_failed_if_claimed_twice() {
        let keyapir = Keypair::new();
        let token_store = InMemoryTokenStore::default();
        let token = token_store
            .generate_new(keyapir.pubkey(), Duration::from_secs(60))
            .await;
        assert!(matches!(token_store.claim(token.clone()).await, Ok(())));
        assert!(matches!(
            token_store.claim(token).await,
            Err(OneTimeAuthTokenClaimError::NotFound)
        ));
    }
}
