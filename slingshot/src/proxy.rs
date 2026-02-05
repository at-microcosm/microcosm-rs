use crate::{Identity, Repo, error::ProxyError, server::HydrationSource};
use atrium_api::types::string::{Did, Nsid};
use reqwest::Client;
use serde_json::{Map, Value};
use std::{collections::HashMap, time::Duration};
use url::Url;

pub enum ParamValue {
    String(Vec<String>),
    Int(Vec<i64>),
    Bool(Vec<bool>),
}
pub struct Params(HashMap<String, ParamValue>);

impl TryFrom<Map<String, Value>> for Params {
    type Error = (); // TODO
    fn try_from(val: Map<String, Value>) -> Result<Self, Self::Error> {
        let mut out = HashMap::new();
        for (k, v) in val {
            match v {
                Value::String(s) => out.insert(k, ParamValue::String(vec![s])),
                Value::Bool(b) => out.insert(k, ParamValue::Bool(vec![b])),
                Value::Number(n) => {
                    let Some(i) = n.as_i64() else {
                        return Err(());
                    };
                    out.insert(k, ParamValue::Int(vec![i]))
                }
                Value::Array(a) => {
                    let Some(first) = a.first() else {
                        continue;
                    };
                    if first.is_string() {
                        let mut vals = Vec::with_capacity(a.len());
                        for v in a {
                            let Some(v) = v.as_str() else {
                                return Err(());
                            };
                            vals.push(v.to_string());
                        }
                        out.insert(k, ParamValue::String(vals));
                    } else if first.is_i64() {
                        let mut vals = Vec::with_capacity(a.len());
                        for v in a {
                            let Some(v) = v.as_i64() else {
                                return Err(());
                            };
                            vals.push(v);
                        }
                        out.insert(k, ParamValue::Int(vals));
                    } else if first.is_boolean() {
                        let mut vals = Vec::with_capacity(a.len());
                        for v in a {
                            let Some(v) = v.as_bool() else {
                                return Err(());
                            };
                            vals.push(v);
                        }
                        out.insert(k, ParamValue::Bool(vals));
                    }
                    todo!();
                }
                _ => return Err(()),
            };
        }

        Ok(Self(out))
    }
}

#[derive(Clone)]
pub struct Proxy {
    repo: Repo,
    identity: Identity,
    client: Client,
}

impl Proxy {
    pub fn new(repo: Repo, identity: Identity) -> Self {
        let client = Client::builder()
            .user_agent(format!(
                "microcosm slingshot v{} (contact: @bad-example.com)",
                env!("CARGO_PKG_VERSION")
            ))
            .no_proxy()
            .timeout(Duration::from_secs(6))
            .build()
            .unwrap();
        Self {
            repo,
            client,
            identity,
        }
    }

    pub async fn proxy(
        &self,
        service_did: &Did,
        service_id: &str,
        xrpc: &Nsid,
        params: Option<Map<String, Value>>,
    ) -> Result<Value, ProxyError> {
        let mut upstream: Url = self
            .identity
            .did_to_mini_service_doc(service_did)
            .await?
            .ok_or(ProxyError::ServiceNotFound)?
            .get(service_id, None)
            .ok_or(ProxyError::ServiceNotMatched)?
            .endpoint
            .parse()?;

        upstream.set_path(&format!("/xrpc/{}", xrpc.as_str()));

        if let Some(params) = params {
            let mut query = upstream.query_pairs_mut();
            let Params(ps) = params.try_into().expect("valid params");
            for (k, pvs) in ps {
                match pvs {
                    ParamValue::String(s) => {
                        for s in s {
                            query.append_pair(&k, &s);
                        }
                    }
                    ParamValue::Int(i) => {
                        for i in i {
                            query.append_pair(&k, &i.to_string());
                        }
                    }
                    ParamValue::Bool(b) => {
                        for b in b {
                            query.append_pair(&k, &b.to_string());
                        }
                    }
                }
            }
        }

        // TODO: other headers to proxy
        Ok(self
            .client
            .get(upstream)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

#[derive(Debug, PartialEq)]
pub enum PathPart {
    Scalar(String),
    Vector(String, Option<String>), // key, $type
}

pub fn parse_record_path(input: &str) -> Result<Vec<PathPart>, String> {
    let mut out = Vec::new();

    let mut key_acc = String::new();
    let mut type_acc = String::new();
    let mut in_bracket = false;
    let mut chars = input.chars().enumerate();
    while let Some((i, c)) = chars.next() {
        match c {
            '[' if in_bracket => return Err(format!("nested opening bracket not allowed, at {i}")),
            '[' if key_acc.is_empty() => {
                return Err(format!("missing key before opening bracket, at {i}"));
            }
            '[' => in_bracket = true,
            ']' if in_bracket => {
                in_bracket = false;
                let key = std::mem::take(&mut key_acc);
                let r#type = std::mem::take(&mut type_acc);
                let t = if r#type.is_empty() {
                    None
                } else {
                    Some(r#type)
                };
                out.push(PathPart::Vector(key, t));
                // peek ahead because we need a dot after array if there's more and i don't want to add more loop state
                let Some((i, c)) = chars.next() else {
                    break;
                };
                if c != '.' {
                    return Err(format!(
                        "expected dot after close bracket, found {c:?} at {i}"
                    ));
                }
            }
            ']' => return Err(format!("unexpected close bracket at {i}")),
            '.' if in_bracket => type_acc.push(c),
            '.' if key_acc.is_empty() => {
                return Err(format!("missing key before next segment, at {i}"));
            }
            '.' => {
                let key = std::mem::take(&mut key_acc);
                assert!(type_acc.is_empty());
                out.push(PathPart::Scalar(key));
            }
            _ if in_bracket => type_acc.push(c),
            _ => key_acc.push(c),
        }
    }
    if in_bracket {
        return Err("unclosed bracket".into());
    }
    if !key_acc.is_empty() {
        out.push(PathPart::Scalar(key_acc));
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RefShape {
    StrongRef,
    AtUri,
    AtUriParts,
    Did,
    Handle,
    AtIdentifier,
}

impl TryFrom<&str> for RefShape {
    type Error = String;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "strong-ref" => Ok(Self::StrongRef),
            "at-uri" => Ok(Self::AtUri),
            "at-uri-parts" => Ok(Self::AtUriParts),
            "did" => Ok(Self::Did),
            "handle" => Ok(Self::Handle),
            "at-identifier" => Ok(Self::AtIdentifier),
            _ => Err(format!("unknown shape: {s}")),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum MatchedRef {
    AtUri { uri: String, cid: Option<String> },
    Identifier(String),
}

pub fn match_shape(shape: RefShape, val: &Value) -> Option<MatchedRef> {
    // TODO: actually validate at-uri format
    // TODO: actually validate everything else also
    // TODO: should this function normalize identifiers to DIDs probably?
    // or just return at-uri parts so the caller can resolve and reassemble
    match shape {
        RefShape::StrongRef => {
            let o = val.as_object()?;
            let uri = o.get("uri")?.as_str()?.to_string();
            let cid = o.get("cid")?.as_str()?.to_string();
            Some(MatchedRef::AtUri {
                uri,
                cid: Some(cid),
            })
        }
        RefShape::AtUri => {
            let uri = val.as_str()?.to_string();
            Some(MatchedRef::AtUri { uri, cid: None })
        }
        RefShape::AtUriParts => {
            let o = val.as_object()?;
            let identifier = o.get("repo").or(o.get("did"))?.as_str()?.to_string();
            let collection = o.get("collection")?.as_str()?.to_string();
            let rkey = o.get("rkey")?.as_str()?.to_string();
            let uri = format!("at://{identifier}/{collection}/{rkey}");
            let cid = o.get("cid").and_then(|v| v.as_str()).map(str::to_string);
            Some(MatchedRef::AtUri { uri, cid })
        }
        RefShape::Did => {
            let id = val.as_str()?;
            if !id.starts_with("did:") {
                return None;
            }
            Some(MatchedRef::Identifier(id.to_string()))
        }
        RefShape::Handle => {
            let id = val.as_str()?;
            if id.contains(':') {
                return None;
            }
            Some(MatchedRef::Identifier(id.to_string()))
        }
        RefShape::AtIdentifier => Some(MatchedRef::Identifier(val.as_str()?.to_string())),
    }
}

// TODO: send back metadata about the matching
pub fn extract_links(
    sources: Vec<HydrationSource>,
    skeleton: &Value,
) -> Result<Vec<MatchedRef>, String> {
    // collect early to catch errors from the client
    // (TODO maybe the handler should do this and pass in the processed stuff probably definitely yeah)
    let sources = sources
        .into_iter()
        .map(|HydrationSource { path, shape }| {
            let path_parts = parse_record_path(&path)?;
            let shape: RefShape = shape.as_str().try_into()?;
            Ok((path_parts, shape))
        })
        .collect::<Result<Vec<_>, String>>()?;

    // lazy first impl, just re-walk the skeleton as many times as needed
    // not deduplicating for now
    let mut out = Vec::new();
    for (path_parts, shape) in sources {
        for val in PathWalker::new(&path_parts, skeleton) {
            if let Some(matched) = match_shape(shape, val) {
                out.push(matched);
            }
        }
    }

    Ok(out)
}

struct PathWalker<'a> {
    todo: Vec<(&'a [PathPart], &'a Value)>,
}
impl<'a> PathWalker<'a> {
    fn new(path_parts: &'a [PathPart], skeleton: &'a Value) -> Self {
        Self {
            todo: vec![(path_parts, skeleton)],
        }
    }
}
impl<'a> Iterator for PathWalker<'a> {
    type Item = &'a Value;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (parts, val) = self.todo.pop()?;
            let Some((part, rest)) = parts.split_first() else {
                return Some(val);
            };
            let Some(o) = val.as_object() else {
                continue;
            };
            match part {
                PathPart::Scalar(k) => {
                    let Some(v) = o.get(k) else {
                        continue;
                    };
                    self.todo.push((rest, v));
                }
                PathPart::Vector(k, t) => {
                    let Some(a) = o.get(k).and_then(|v| v.as_array()) else {
                        continue;
                    };
                    for v in a.iter().rev().filter(|c| {
                        let Some(t) = t else { return true };
                        c.as_object()
                            .and_then(|o| o.get("$type"))
                            .and_then(|v| v.as_str())
                            .map(|s| s == t)
                            .unwrap_or(false)
                    }) {
                        self.todo.push((rest, v))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_record_path() -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            ("", vec![]),
            ("subject", vec![PathPart::Scalar("subject".into())]),
            ("authorDid", vec![PathPart::Scalar("authorDid".into())]),
            (
                "subject.uri",
                vec![
                    PathPart::Scalar("subject".into()),
                    PathPart::Scalar("uri".into()),
                ],
            ),
            ("members[]", vec![PathPart::Vector("members".into(), None)]),
            (
                "add[].key",
                vec![
                    PathPart::Vector("add".into(), None),
                    PathPart::Scalar("key".into()),
                ],
            ),
            ("a[b]", vec![PathPart::Vector("a".into(), Some("b".into()))]),
            (
                "a[b.c]",
                vec![PathPart::Vector("a".into(), Some("b.c".into()))],
            ),
            (
                "facets[app.bsky.richtext.facet].features[app.bsky.richtext.facet#mention].did",
                vec![
                    PathPart::Vector("facets".into(), Some("app.bsky.richtext.facet".into())),
                    PathPart::Vector(
                        "features".into(),
                        Some("app.bsky.richtext.facet#mention".into()),
                    ),
                    PathPart::Scalar("did".into()),
                ],
            ),
        ];

        for (path, expected) in cases {
            let parsed = parse_record_path(path)?;
            assert_eq!(parsed, expected, "path: {path:?}");
        }

        Ok(())
    }

    #[test]
    fn test_match_shape() {
        let cases = [
            ("strong-ref", json!(""), None),
            ("strong-ref", json!({}), None),
            ("strong-ref", json!({ "uri": "abc" }), None),
            ("strong-ref", json!({ "cid": "def" }), None),
            (
                "strong-ref",
                json!({ "uri": "abc", "cid": "def" }),
                Some(MatchedRef::AtUri {
                    uri: "abc".to_string(),
                    cid: Some("def".to_string()),
                }),
            ),
            ("at-uri", json!({ "uri": "abc" }), None),
            ("at-uri", json!({ "uri": "abc", "cid": "def" }), None),
            (
                "at-uri",
                json!("abc"),
                Some(MatchedRef::AtUri {
                    uri: "abc".to_string(),
                    cid: None,
                }),
            ),
            ("at-uri-parts", json!("abc"), None),
            ("at-uri-parts", json!({}), None),
            (
                "at-uri-parts",
                json!({"repo": "a", "collection": "b", "rkey": "c"}),
                Some(MatchedRef::AtUri {
                    uri: "at://a/b/c".to_string(),
                    cid: None,
                }),
            ),
            (
                "at-uri-parts",
                json!({"did": "a", "collection": "b", "rkey": "c"}),
                Some(MatchedRef::AtUri {
                    uri: "at://a/b/c".to_string(),
                    cid: None,
                }),
            ),
            (
                "at-uri-parts",
                // 'repo' takes precedence over 'did'
                json!({"did": "a", "repo": "z", "collection": "b", "rkey": "c"}),
                Some(MatchedRef::AtUri {
                    uri: "at://z/b/c".to_string(),
                    cid: None,
                }),
            ),
            (
                "at-uri-parts",
                json!({"repo": "a", "collection": "b", "rkey": "c", "cid": "def"}),
                Some(MatchedRef::AtUri {
                    uri: "at://a/b/c".to_string(),
                    cid: Some("def".to_string()),
                }),
            ),
            (
                "at-uri-parts",
                json!({"repo": "a", "collection": "b", "rkey": "c", "cid": {}}),
                Some(MatchedRef::AtUri {
                    uri: "at://a/b/c".to_string(),
                    cid: None,
                }),
            ),
            ("did", json!({}), None),
            ("did", json!(""), None),
            ("did", json!("bad-example.com"), None),
            (
                "did",
                json!("did:plc:xyz"),
                Some(MatchedRef::Identifier("did:plc:xyz".to_string())),
            ),
            ("handle", json!({}), None),
            (
                "handle",
                json!("bad-example.com"),
                Some(MatchedRef::Identifier("bad-example.com".to_string())),
            ),
            ("handle", json!("did:plc:xyz"), None),
            ("at-identifier", json!({}), None),
            (
                "at-identifier",
                json!("bad-example.com"),
                Some(MatchedRef::Identifier("bad-example.com".to_string())),
            ),
            (
                "at-identifier",
                json!("did:plc:xyz"),
                Some(MatchedRef::Identifier("did:plc:xyz".to_string())),
            ),
        ];
        for (shape, val, expected) in cases {
            let s = shape.try_into().unwrap();
            let matched = match_shape(s, &val);
            assert_eq!(matched, expected, "shape: {shape:?}, val: {val:?}");
        }
    }
}
