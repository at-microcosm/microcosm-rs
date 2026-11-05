/// see https://atproto.com/specs/did#at-protocol-did-identifier-syntax
/// this parser is intentinonally lax: it should accept all valid DIDs, and
/// may accept some invalid DIDs.
///
/// at the moment this implementation might also be quite bad and incomplete
pub fn parse_did(s: &str) -> Option<String> {
    // for now, just working through the rules laid out in the docs in order,
    // without much regard for efficiency for now.

    // newer specs say max 2048 chars
    if s.len() > 2048 {
        return None;
    }

    // The entire URI is made up of a subset of ASCII, containing letters (A-Z, a-z),
    // digits (0-9), period, underscore, colon, percent sign, or hyphen (._:%-)
    if !s
        .chars()
        .all(|c| matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | '.' | '_' | ':' | '%' | '-'))
    {
        return None;
    }

    // The URI is case-sensitive
    // -> (nothing to check)

    // The URI starts with lowercase `did:`
    let unprefixed = s.strip_prefix("did:")?;

    // The method segment is one or more lowercase letters (a-z), followed by :
    let (method, identifier) = unprefixed.split_once(':')?;
    if !method.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }

    // The remainder of the URI (the identifier) may contain any of the above-allowed
    // ASCII characters, except for percent-sign (%)
    // -> ok, ugh, gotta know our encoding context for this

    // The URI (and thus the remaining identifier) may not end in ':'.
    if identifier.ends_with(':') {
        return None;
    }

    // Percent-sign (%) is used for "percent encoding" in the identifier section, and
    // must always be followed by two hex characters
    // -> again incoding context (bleh)

    // Query (?) and fragment (#) sections are allowed in DID URIs, but not in DID
    // identifiers. In the context of atproto, the query and fragment parts are not
    // allowed.
    // -> disallow here -- the uri decoder should already split them out first.

    // DID identifiers do not generally have a maximum length restriction, but in the
    // context of atproto, there is an initial hard limit of 2 KB.
    // -> we're in atproto, so sure, let's enforce it. (would be sensible to do this
    // ->   first but we're following doc order)
    if s.len() > (2 * 2_usize.pow(10)) {
        return None;
    }

    // -> it's not actually written in the spec, but by example in the spec, the
    // -> identifier cannot be empty
    if identifier.is_empty() {
        return None;
    }

    Some(s.to_string())
    // the only normalization we might want would be percent-decoding, but we
    // probably leave that to the uri decoder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_did_too_long() {
        let long = concat!(
            "did:long:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        );
        assert_eq!(parse_did(long), None);
    }

    #[test]
    fn test_did_parse() {
        for (case, expected, detail) in vec![
            ("", None, "empty str"),
            (" ", None, "whitespace str"),
            ("z", None, "not a did"),
            ("did:plc", None, "no identifier separator colon"),
            ("did:plc:", None, "missing identifier"),
            (
                "did:web:bad-example.com",
                Some("did:web:bad-example.com"),
                "web did",
            ),
            (
                "did:plc:hdhoaan3xa3jiuq4fg4mefid",
                Some("did:plc:hdhoaan3xa3jiuq4fg4mefid"),
                "plc did",
            ),
            (
                "DID:plc:hdhoaan3xa3jiuq4fg4mefid",
                None,
                "'did:' prefix must be lowercase",
            ),
            (
                "did:ok:z",
                Some("did:ok:z"),
                "unknown did methods are allowed",
            ),
            ("did:BAD:z", None, "non-lowercase methods are not allowed"),
            ("did:bad:z$z", None, "invalid chars are not allowed"),
            (
                "did:ok:z:z",
                Some("did:ok:z:z"),
                "colons are allowed in identifier",
            ),
            ("did:bad:z:", None, "colons not are allowed at the end"),
            ("did:bad:z?q=y", None, "queries are not allowed in atproto"),
            ("did:bad:z#a", None, "anchors are not allowed in atproto"),
        ] {
            assert_eq!(parse_did(case), expected.map(|s| s.to_string()), "{detail}");
        }
    }

    #[test]
    fn test_doc_exmples_atproto() {
        // https://atproto.com/specs/did#at-protocol-did-identifier-syntax
        for case in ["did:plc:z72i7hdynmk6r22z27h6tvur", "did:web:blueskyweb.xyz"] {
            assert!(parse_did(case).is_some(), "should pass: {case}")
        }
    }

    #[test]
    fn test_doc_exmples_lexicon() {
        // https://atproto.com/specs/did#at-protocol-did-identifier-syntax
        for case in [
            "did:method:val:two",
            "did:m:v",
            "did:method::::val",
            "did:method:-:_:.",
            "did:key:zQ3shZc2QzApp2oymGvQbzP8eKheVshBHbU4ZYjeXqwSKEn6N",
        ] {
            assert!(parse_did(case).is_some(), "should pass: {case}")
        }
    }

    #[test]
    fn test_doc_exmples_invalid() {
        // https://atproto.com/specs/did#at-protocol-did-identifier-syntax
        for case in [
            "did:METHOD:val",
            "did:m123:val",
            "DID:method:val",
            "did:method:",
            "did:method:val/two",
            "did:method:val?two",
            "did:method:val#two",
        ] {
            assert!(parse_did(case).is_none(), "should fail: {case}")
        }
    }
}
