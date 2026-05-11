use microcosm_links::{parse_any_link, Link};
use num_format::{Locale, ToFormattedString};

pub fn to_browseable(s: &str) -> askama::Result<Option<String>> {
    Ok({
        if let Some(link) = parse_any_link(s) {
            match link {
                Link::AtUri(at_uri) => at_uri
                    .strip_prefix("at://")
                    .map(|noproto| format!("https://pdsls.dev/at://{noproto}")),
                Link::Did(did) => Some(format!("https://pdsls.dev/at://{did}")),
                Link::Uri(uri) => Some(uri),
            }
        } else {
            None
        }
    })
}

pub fn human_number(n: &u64) -> askama::Result<String> {
    Ok(n.to_formatted_string(&Locale::en))
}

pub fn to_u64(n: usize) -> askama::Result<u64> {
    Ok(n as u64)
}
