use atrium_crypto::did::parse_multikey;
use atrium_crypto::verify::Verifier;
use jwt_compact::UntrustedToken;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Deserialize)]
struct MiniDoc {
    signing_key: String,
    did: String,
}

#[derive(Error, Debug)]
pub enum VerifyError {
    #[error("The cross-service authorization token failed verification: {0}")]
    VerificationFailed(&'static str),
    #[error("Error trying to resolve the DID to a signing key, retry in a moment: {0}")]
    ResolutionFailed(&'static str),
}

pub struct TokenVerifier {
    client: reqwest::Client,
}

impl TokenVerifier {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .user_agent(format!(
                "microcosm pocket v{} (dev: @bad-example.com)",
                env!("CARGO_PKG_VERSION")
            ))
            .no_proxy()
            .timeout(Duration::from_secs(12)) // slingshot timeout is 10s
            .build()
            .unwrap();
        Self { client }
    }

    pub async fn verify(
        &self,
        expected_lxm: &str,
        token: &str,
    ) -> Result<(String, String), VerifyError> {
        let untrusted = UntrustedToken::new(token).unwrap();

        // danger! unfortunately we need to decode the DID from the jwt body before we have a public key to verify the jwt with
        let Ok(untrusted_claims) =
            untrusted.deserialize_claims_unchecked::<HashMap<String, String>>()
        else {
            return Err(VerifyError::VerificationFailed(
                "could not deserialize jtw claims",
            ));
        };

        // get the (untrusted!) claimed DID
        let Some(untrusted_did) = untrusted_claims.custom.get("iss") else {
            return Err(VerifyError::VerificationFailed(
                "jwt must include the user's did in `iss`",
            ));
        };

        // bail if it's not even a user-ish did
        if !untrusted_did.starts_with("did:") {
            return Err(VerifyError::VerificationFailed("iss should be a did"));
        }
        if untrusted_did.contains("#") {
            return Err(VerifyError::VerificationFailed(
                "iss should be a user did without a service identifier",
            ));
        }

        let endpoint =
            "https://slingshot.microcosm.blue/xrpc/com.bad-example.identity.resolveMiniDoc";
        let doc: MiniDoc = self
            .client
            .get(format!("{endpoint}?identifier={untrusted_did}"))
            .send()
            .await
            .map_err(|_| VerifyError::ResolutionFailed("failed to fetch minidoc"))?
            .error_for_status()
            .map_err(|_| VerifyError::ResolutionFailed("non-ok response for minidoc"))?
            .json()
            .await
            .map_err(|_| VerifyError::ResolutionFailed("failed to parse json to minidoc"))?;

        // sanity check before we go ahead with this signing key
        if doc.did != *untrusted_did {
            return Err(VerifyError::VerificationFailed(
                "wtf, resolveMiniDoc returned a doc for a different DID, slingshot bug",
            ));
        }

        let Ok((alg, public_key)) = parse_multikey(&doc.signing_key) else {
            return Err(VerifyError::VerificationFailed(
                "could not parse signing key form minidoc",
            ));
        };

        // i _guess_ we've successfully bootstrapped the verification of the jwt unless this fails
        if let Err(e) = Verifier::default().verify(
            alg,
            &public_key,
            &untrusted.signed_data,
            untrusted.signature_bytes(),
        ) {
            log::warn!("jwt verification failed: {e}");
            return Err(VerifyError::VerificationFailed(
                "jwt signature verification failed",
            ));
        }

        // past this point we're should have established trust. crossing ts and dotting is.
        let did = &untrusted_did;
        let claims = &untrusted_claims;

        let Some(aud) = claims.custom.get("aud") else {
            return Err(VerifyError::VerificationFailed("missing aud"));
        };
        let Some(mut aud) = aud.strip_prefix("did:web:") else {
            return Err(VerifyError::VerificationFailed("expected a did:web aud"));
        };
        if let Some((aud_without_hash, _)) = aud.split_once("#") {
            log::warn!("aud claim is missing service id fragment: {aud:?}");
            aud = aud_without_hash;
        }
        let Some(lxm) = claims.custom.get("lxm") else {
            return Err(VerifyError::VerificationFailed("missing lxm"));
        };
        if lxm != expected_lxm {
            return Err(VerifyError::VerificationFailed("wrong lxm"));
        }

        Ok((did.to_string(), aud.to_string()))
    }
}

impl Default for TokenVerifier {
    fn default() -> Self {
        Self::new()
    }
}
