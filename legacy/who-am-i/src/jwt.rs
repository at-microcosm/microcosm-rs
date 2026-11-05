use elliptic_curve::SecretKey;
use jose_jwk::{Class, Jwk, Key, Parameters};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, errors::Error as JWTError};
use pkcs8::DecodePrivateKey;
use serde::Serialize;
use std::fs;
use std::io::Error as IOError;
use std::path::Path;
use std::string::FromUtf8Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TokensSetupError {
    #[error("failed to read private key")]
    ReadPrivateKey(IOError),
    #[error("failed to retrieve private key: {0}")]
    PrivateKey(JWTError),
    #[error("failed to read private key")]
    ReadJwks(IOError),
    #[error("failed to retrieve jwks: {0}")]
    DecodeJwks(FromUtf8Error),
}

#[derive(Debug, Error)]
pub enum TokenMintingError {
    #[error("failed to mint: {0}")]
    EncodingError(#[from] JWTError),
}

pub struct Tokens {
    encoding_key: EncodingKey,
    jwk: Jwk,
}

impl Tokens {
    pub fn from_files(priv_f: impl AsRef<Path>) -> Result<Self, TokensSetupError> {
        let private_key_data: Vec<u8> =
            fs::read(priv_f).map_err(TokensSetupError::ReadPrivateKey)?;
        let encoding_key =
            EncodingKey::from_ec_pem(&private_key_data).map_err(TokensSetupError::PrivateKey)?;

        let jwk_key_string = String::from_utf8(private_key_data).unwrap();
        let mut jwk = SecretKey::<p256::NistP256>::from_pkcs8_pem(&jwk_key_string)
            .map(|secret_key| Jwk {
                key: Key::from(&secret_key.into()),
                prm: Parameters {
                    kid: Some("who-am-i-00".to_string()),
                    cls: Some(Class::Signing),
                    ..Default::default()
                },
            })
            .expect("to get private key");

        // CRITICAL: this is what turns the private jwk into a public one: the
        // `d` parameter is the secret for an EC key; a pubkey just has no `d`.
        //
        // this feels baaaadd but hey we're just copying atrium
        // https://github.com/atrium-rs/atrium/blob/b48810f84d83d037ee89b79b8566df9e0f2a6dae/atrium-oauth/src/keyset.rs#L41
        let Key::Ec(ref mut ec) = jwk.key else {
            unimplemented!()
        };
        ec.d = None; // CRITICAL

        Ok(Self { encoding_key, jwk })
    }

    pub fn mint(&self, t: impl ToString) -> Result<String, TokenMintingError> {
        let sub = t.to_string();

        let dt_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("unix epoch is in the past");
        let dt_exp = dt_now + Duration::from_secs(30 * 86_400);
        let exp = dt_exp.as_secs();

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some("who-am-i-00".to_string());
        // todo: consider setting jku?

        Ok(encode(&header, &Claims { sub, exp }, &self.encoding_key)?)
    }

    pub fn jwk(&self) -> Jwk {
        self.jwk.clone()
    }
}

#[derive(Debug, Serialize)]
struct Claims {
    sub: String,
    exp: u64,
}
