#[derive(Debug, thiserror::Error, PartialEq)]
pub enum LinkSourceError {
    #[error("Collection-path separator `:` is required")]
    MissingSeparator,
    #[error("Collection before `:` is required")]
    MissingCollection,
    #[error("Record path or `.` after `:'`is required")]
    MissingPath,
    #[error("Leading dot in path is not allowed")]
    LeadingPathDot,
}

/// parse a record path (or rkey sentinel)
pub fn parse_path(input: &str) -> Result<String, LinkSourceError> {
    match input {
        "" => Err(LinkSourceError::MissingPath),
        "." => Ok(".".to_string()),
        p if p.starts_with('.') => Err(LinkSourceError::LeadingPathDot),
        p => Ok(format!(".{p}")),
    }
}

/// hacky version that will eventually be from a nicer library
///
/// syntax: `<NSID>:<RecordPath OR '.'>`
///
/// right now `NSID` is not validated, but it will be soon.
///
/// right now `RecordPath` is just a string, but it will be replaced by
/// tangled.org/microcosm.blue/RecordPath soon.
///
/// the special `.` path refers to the rkey instead of record contents
///
/// returns: (collection NISD, path), where `path` is always .-prefixed
pub fn parse_link_source(input: &str) -> Result<(String, String), LinkSourceError> {
    let (collection, path) = input
        .split_once(':')
        .ok_or(LinkSourceError::MissingSeparator)?;

    if collection.is_empty() {
        return Err(LinkSourceError::MissingCollection);
    }

    let path = parse_path(path)?;

    Ok((collection.to_string(), path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_link_source() {
        for (case, expected) in [
            ("", Err(LinkSourceError::MissingSeparator)),
            ("a", Err(LinkSourceError::MissingSeparator)),
            (":", Err(LinkSourceError::MissingCollection)),
            ("a:", Err(LinkSourceError::MissingPath)),
            (":a", Err(LinkSourceError::MissingCollection)),
            ("a:b", Ok(("a".to_string(), ".b".to_string()))),
            (
                "app.bsky.feed.like:subject",
                Ok(("app.bsky.feed.like".to_string(), ".subject".to_string())),
            ),
            ("a:.", Ok(("a".to_string(), ".".to_string()))),
            (
                "app.bsky.feed.like:subject.uri",
                Ok(("app.bsky.feed.like".to_string(), ".subject.uri".to_string())),
            ),
            (
                "a:items[].uri",
                Ok(("a".to_string(), ".items[].uri".to_string())),
            ),
            ("a:b:c", Ok(("a".to_string(), ".b:c".to_string()))),
            ("a:.foo", Err(LinkSourceError::LeadingPathDot)),
        ] {
            let res = parse_link_source(case);
            assert_eq!(res, expected);
        }
    }

    #[test]
    fn test_parse_path() {
        for (case, expected) in [
            ("", Err(LinkSourceError::MissingPath)),
            (".", Ok(".".to_string())),
            ("foo", Ok(".foo".to_string())),
            ("foo.bar", Ok(".foo.bar".to_string())),
            ("items[].uri", Ok(".items[].uri".to_string())),
            (".foo", Err(LinkSourceError::LeadingPathDot)),
            ("..", Err(LinkSourceError::LeadingPathDot)),
        ] {
            let res = parse_path(case);
            assert_eq!(res, expected);
        }
    }
}
