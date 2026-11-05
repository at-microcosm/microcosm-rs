use fluent_uri::Uri;

pub mod at_uri;
pub mod did;
#[cfg(feature = "json")]
pub mod record;

#[cfg(feature = "json")]
pub use record::collect_links;

#[derive(Debug, Clone, Ord, Eq, PartialOrd, PartialEq)]
pub enum Link {
    AtUri(String),
    Uri(String),
    Did(String),
}

impl Link {
    pub fn into_string(self) -> String {
        match self {
            Link::AtUri(s) => s,
            Link::Uri(s) => s,
            Link::Did(s) => s,
        }
    }
    pub fn as_str(&self) -> &str {
        match self {
            Link::AtUri(s) => s,
            Link::Uri(s) => s,
            Link::Did(s) => s,
        }
    }
    pub fn name(&self) -> &'static str {
        match self {
            Link::AtUri(_) => "at-uri",
            Link::Uri(_) => "uri",
            Link::Did(_) => "did",
        }
    }
    pub fn at_uri_collection(&self) -> Option<String> {
        if let Link::AtUri(at_uri) = self {
            at_uri::at_uri_collection(at_uri)
        } else {
            None
        }
    }
    pub fn did(&self) -> Option<String> {
        let did = match self {
            Link::AtUri(s) => {
                let rest = s.strip_prefix("at://")?; // todo: this might be safe to unwrap?
                if let Some((did, _)) = rest.split_once("/") {
                    did
                } else {
                    rest
                }
            }
            Link::Uri(_) => return None,
            Link::Did(did) => did,
        };
        Some(did.to_string())
    }
}

#[derive(Debug, PartialEq)]
pub struct CollectedLink {
    pub path: String,
    pub target: Link,
}

// normalizing is a bit opinionated but eh
pub fn parse_uri(s: &str) -> Option<String> {
    Uri::parse(s).map(|u| u.normalize().into_string()).ok()
}

pub fn parse_any_link(s: &str) -> Option<Link> {
    at_uri::parse_at_uri(s).map(Link::AtUri).or_else(|| {
        did::parse_did(s)
            .map(Link::Did)
            .or_else(|| parse_uri(s).map(Link::Uri))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_parse() {
        let s = "https://example.com";
        let uri = parse_uri(s).unwrap();
        assert_eq!(uri.as_str(), s);
    }

    #[test]
    fn test_uri_normalizes() {
        let s = "HTTPS://example.com/../";
        let uri = parse_uri(s).unwrap();
        assert_eq!(uri.as_str(), "https://example.com/");
    }

    #[test]
    fn test_uri_invalid() {
        assert!(parse_uri("https:\\bad-example.com").is_none());
    }

    #[test]
    fn test_any_parse() {
        assert_eq!(
            parse_any_link("https://example.com"),
            Some(Link::Uri("https://example.com".into()))
        );

        assert_eq!(
            parse_any_link(
                "at://did:plc:44ybard66vv44zksje25o7dz/app.bsky.feed.post/3jwdwj2ctlk26"
            ),
            Some(Link::AtUri(
                "at://did:plc:44ybard66vv44zksje25o7dz/app.bsky.feed.post/3jwdwj2ctlk26".into()
            )),
        );

        assert_eq!(
            parse_any_link("did:plc:44ybard66vv44zksje25o7dz"),
            Some(Link::Did("did:plc:44ybard66vv44zksje25o7dz".into()))
        );

        assert_eq!(
            parse_any_link("tel:5551234567"),
            Some(Link::Uri("tel:5551234567".into())),
        );

        assert_eq!(parse_any_link("3jwdwj2ctlk26"), None);
        assert_eq!(parse_any_link("self"), None);
        assert_eq!(parse_any_link(""), None);
    }

    #[test]
    fn test_at_uri_collection() {
        assert_eq!(
            parse_any_link("https://example.com")
                .unwrap()
                .at_uri_collection(),
            None
        );
        assert_eq!(
            parse_any_link("did:web:bad-example.com")
                .unwrap()
                .at_uri_collection(),
            None
        );
        assert_eq!(
            parse_any_link("at://did:web:bad-example.com/my.collection/3jwdwj2ctlk26")
                .unwrap()
                .at_uri_collection(),
            Some("my.collection".into())
        );
    }
}
