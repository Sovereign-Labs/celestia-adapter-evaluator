//! CometBFT consensus-RPC client for the historical `block_results` read loop.
//!
//! This is deliberately independent of the `sov-celestia-adapter` bridge-node RPC
//! used by the submit/sync read loops: it talks to a Tendermint/CometBFT consensus
//! node (`:26657`) via the `tendermint-rpc` crate, the same surface the
//! `forwarding-relayer` scanner uses (`status` + `block_results`).
//!
//! The auth token is spliced into the URL as HTTP Basic userinfo
//! (`scheme://:token@host`), producing `Authorization: Basic base64(":token")` on
//! every request — mirroring `forwarding-relayer`'s `with_auth_token`.

use std::str::FromStr;

use anyhow::{Context, Result};
use tendermint::block::Height;
use tendermint_rpc::{Client, HttpClient};

/// A gateway auth token restricted to URL-safe characters, so it can be spliced
/// verbatim into the RPC URL. Ported from `forwarding-relayer::auth::AuthToken`.
#[derive(Clone, Debug)]
pub struct AuthToken(String);

impl AuthToken {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AuthToken {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        if s.is_empty() {
            return Err("must not be empty".into());
        }
        if let Some(bad) = s
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~')))
        {
            return Err(format!(
                "contains unsupported character {bad:?}; only URL-safe characters are \
                 allowed (ASCII alphanumerics and - _ . ~)"
            ));
        }
        Ok(AuthToken(s.to_string()))
    }
}

/// Splice `token` into `url` as HTTP Basic userinfo in the password field
/// (`scheme://:token@host`). Returns the URL unchanged when no token is given.
/// The token is URL-safe by construction ([`AuthToken`]), so it is inserted verbatim.
fn with_auth_token(url: &str, token: Option<&str>) -> Result<String> {
    let Some(token) = token else {
        return Ok(url.to_string());
    };
    let (scheme, rest) = url
        .split_once("://")
        .with_context(|| format!("Invalid consensus RPC URL (expected scheme://host): {url}"))?;
    Ok(format!("{scheme}://:{token}@{rest}"))
}

/// Strip HTTP Basic userinfo from a URL so a spliced token never reaches logs.
/// Ported from `forwarding-relayer::scanner::redact_userinfo`.
pub fn redact_userinfo(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let auth_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(auth_end);
    match authority.rsplit_once('@') {
        Some((_creds, host)) => format!("{scheme}://***@{host}{tail}"),
        None => url.to_string(),
    }
}

/// Validate an optional raw token string into an [`AuthToken`].
pub fn parse_token(token: Option<&str>) -> Result<Option<AuthToken>> {
    match token {
        None => Ok(None),
        Some(t) => t
            .parse::<AuthToken>()
            .map(Some)
            .map_err(|e| anyhow::anyhow!("invalid --consensus-rpc-token: {e}")),
    }
}

/// Build a CometBFT consensus RPC HTTP client, splicing the token in as Basic auth.
pub fn build_client(endpoint: &str, token: Option<&AuthToken>) -> Result<HttpClient> {
    let conn_url = with_auth_token(endpoint, token.map(AuthToken::as_str))?;
    HttpClient::new(conn_url.as_str())
        .with_context(|| format!("Invalid consensus RPC URL: {}", redact_userinfo(endpoint)))
}

/// The available block height window reported by the node's `status`.
#[derive(Debug, Clone, Copy)]
pub struct HeightWindow {
    /// Oldest height the node still stores (`sync_info.earliest_block_height`).
    pub earliest: u64,
    /// Chain tip (`sync_info.latest_block_height`).
    pub latest: u64,
}

/// Query the node's height window via `status`.
pub async fn fetch_height_window(http: &HttpClient) -> Result<HeightWindow> {
    let status = http
        .status()
        .await
        .context("Failed to query consensus node status")?;
    Ok(HeightWindow {
        earliest: status.sync_info.earliest_block_height.value(),
        latest: status.sync_info.latest_block_height.value(),
    })
}

/// Summary of one `block_results` response — what we measure per read.
#[derive(Debug, Clone, Copy)]
pub struct BlockResultsSummary {
    pub num_txs: u64,
    pub num_events: u64,
}

/// Fetch `block_results` for `height` and summarize it (tx count + total ABCI
/// events across tx, finalize, begin and end collections — same collections the
/// relayer scans).
pub async fn read_block_results(http: &HttpClient, height: u64) -> Result<BlockResultsSummary> {
    let h = Height::try_from(height).with_context(|| format!("invalid block height {height}"))?;
    let results = http
        .block_results(h)
        .await
        .with_context(|| format!("Failed to fetch block_results for height {height}"))?;

    let tx_events: usize = results
        .txs_results
        .iter()
        .flatten()
        .map(|tx| tx.events.len())
        .sum();
    let num_events = tx_events
        + results.finalize_block_events.len()
        + results.begin_block_events.iter().flatten().count()
        + results.end_block_events.iter().flatten().count();
    let num_txs = results.txs_results.iter().flatten().count();

    Ok(BlockResultsSummary {
        num_txs: num_txs as u64,
        num_events: num_events as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splices_token_as_basic_auth() {
        assert_eq!(
            with_auth_token("http://host:26657", Some("tok123")).unwrap(),
            "http://:tok123@host:26657"
        );
    }

    #[test]
    fn no_token_leaves_url_unchanged() {
        assert_eq!(
            with_auth_token("http://host:26657", None).unwrap(),
            "http://host:26657"
        );
    }

    #[test]
    fn redacts_spliced_userinfo() {
        assert_eq!(
            redact_userinfo("http://:secret@host:26657/websocket"),
            "http://***@host:26657/websocket"
        );
        assert_eq!(redact_userinfo("http://host:26657"), "http://host:26657");
    }

    #[test]
    fn rejects_url_breaking_tokens() {
        for bad in ["ab/cd", "ab@cd", "a:b", "tok en", ""] {
            assert!(
                bad.parse::<AuthToken>().is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn accepts_url_safe_tokens() {
        for ok in ["abcDEF0123", "A-z_0.9~"] {
            assert!(ok.parse::<AuthToken>().is_ok(), "{ok} should be valid");
        }
    }
}
