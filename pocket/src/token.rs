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
    // client errors
    #[error("Bad cross-service authorization token: {0}")]
    BadToken(&'static str),
    #[error("Could not find that identity")]
    ProbablyBadIdentity,
    // server errors
    #[error("The cross-service authorization token failed verification: {0}")]
    FailedToVerify(&'static str),
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
        expected_aud: &str,
        token: &str,
    ) -> Result<String, VerifyError> {
        let untrusted = UntrustedToken::new(token).unwrap();

        // danger! unfortunately we need to decode the DID from the jwt body before we have a public key to verify the jwt with
        let Ok(untrusted_claims) =
            untrusted.deserialize_claims_unchecked::<HashMap<String, String>>()
        else {
            return Err(VerifyError::BadToken("could not deserialize jtw claims"));
        };

        // get the (untrusted!) claimed DID
        let Some(untrusted_did) = untrusted_claims.custom.get("iss") else {
            return Err(VerifyError::BadToken(
                "jwt must include the user's did in `iss`",
            ));
        };

        // bail if it's not even a user-ish did
        if !untrusted_did.starts_with("did:") {
            return Err(VerifyError::BadToken("iss should be a did"));
        }
        if untrusted_did.contains("#") {
            return Err(VerifyError::BadToken(
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
            .map_err(|_| VerifyError::FailedToVerify("failed to fetch minidoc"))?
            .error_for_status()
            .map_err(|_| VerifyError::ProbablyBadIdentity)?
            .json()
            .await
            .map_err(|_| VerifyError::FailedToVerify("failed to parse json to minidoc"))?;

        // sanity check before we go ahead with this signing key
        if doc.did != *untrusted_did {
            return Err(VerifyError::FailedToVerify(
                "resolveMiniDoc returned a doc for a different DID: slingshot bug??",
            ));
        }

        let Ok((alg, public_key)) = parse_multikey(&doc.signing_key) else {
            return Err(VerifyError::FailedToVerify(
                "could not parse signing key from minidoc",
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
            return Err(VerifyError::BadToken("jwt signature verification failed"));
        }

        // past this point we've established that the token is authentic. crossing ts and dotting is.
        let did = &untrusted_did;
        let claims = &untrusted_claims;

        let Some(aud) = claims.custom.get("aud") else {
            return Err(VerifyError::BadToken("missing aud"));
        };
        let Some(mut aud) = aud.strip_prefix("did:web:") else {
            return Err(VerifyError::BadToken("expected a did:web aud"));
        };
        if let Some((aud_without_hash, _)) = aud.split_once("#") {
            log::warn!("aud claim is missing service id fragment: {aud:?}");
            aud = aud_without_hash;
        }
        if aud != expected_aud {
            return Err(VerifyError::BadToken("wrong aud"));
        }
        let Some(lxm) = claims.custom.get("lxm") else {
            return Err(VerifyError::BadToken("missing lxm"));
        };
        if lxm != expected_lxm {
            return Err(VerifyError::BadToken("wrong lxm"));
        }

        Ok(did.to_string())
    }
}

impl Default for TokenVerifier {
    fn default() -> Self {
        Self::new()
    }
}
