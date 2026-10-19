//! Tokens from a registry's token service, which registries ask for even for
//! public repositories.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Context, bail, ensure};
use serde::Deserialize;
use url::Url;

use crate::transport;

/// Far more than a token service's answer needs.
const MAX_TOKEN: usize = 1 << 20;

/// The credentials for one repository.
pub struct Credentials {
    /// `host` or `host:port`.
    host: String,
    /// What the token service grants, like `repository:<name>:pull`.
    scope: String,
    /// The token service's last token.
    token: Mutex<Option<String>>,
    /// Hosts whose token service may be plain HTTP.
    insecure: Vec<String>,
}

/// What a token service answers. Docker's registry names the token `token`,
/// and OAuth 2 names it `access_token`.
#[derive(Deserialize)]
struct TokenAnswer {
    token: Option<String>,
    access_token: Option<String>,
}

impl Credentials {
    pub fn new(host: &str, scope: String, insecure: Vec<String>) -> Self {
        Self {
            host: host.to_owned(),
            scope,
            token: Mutex::default(),
            insecure,
        }
    }

    /// Adds the token to a request for `url`. A request to another host, like
    /// a redirect to blob storage, doesn't get it.
    pub fn authorize(
        &self,
        request: reqwest::RequestBuilder,
        url: &Url,
    ) -> reqwest::RequestBuilder {
        if authority(url) != self.host {
            return request;
        }
        match &*self.token.lock().unwrap() {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    /// Answers a 401's `WWW-Authenticate`. For `Bearer`, it gets an anonymous
    /// token from the service the challenge names, and requests use it from
    /// then on.
    pub async fn answer(&self, challenge: &str) -> anyhow::Result<()> {
        let (scheme, params) = parse_challenge(challenge)
            .with_context(|| format!("unsupported WWW-Authenticate {challenge:?}"))?;
        if scheme.eq_ignore_ascii_case("basic") {
            bail!("the registry wants a login for {}", self.host);
        }
        ensure!(
            scheme.eq_ignore_ascii_case("bearer"),
            "unsupported WWW-Authenticate scheme {scheme}"
        );
        let realm = params
            .get("realm")
            .context("WWW-Authenticate has no realm")?;
        let mut url = Url::parse(realm).with_context(|| format!("realm {realm:?}"))?;
        let host = url.host_str().unwrap_or_default();
        let plain_ok = (self.insecure.iter())
            .any(|h| h.eq_ignore_ascii_case(host) || h.eq_ignore_ascii_case(&authority(&url)));
        ensure!(
            url.scheme() == "https" || (url.scheme() == "http" && plain_ok),
            "the token service {realm} is not https://"
        );
        {
            let mut query = url.query_pairs_mut();
            if let Some(service) = params.get("service") {
                query.append_pair("service", service);
            }
            // The registry's scope is the one it wants, like `pull,push` for
            // an upload.
            query.append_pair("scope", params.get("scope").unwrap_or(&self.scope));
        }
        let request = transport::client()?.get(url.clone());
        let resp = request.send().await.map_err(reqwest::Error::without_url)?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            bail!("the token service wants a login for {}", self.host);
        }
        ensure!(status.is_success(), "the token service answered {status}");
        let body = resp.bytes().await.map_err(reqwest::Error::without_url)?;
        ensure!(
            body.len() <= MAX_TOKEN,
            "the token service's answer is too big"
        );
        let answer: TokenAnswer =
            serde_json::from_slice(&body).context("parsing the token service's answer")?;
        let token = (answer.token.or(answer.access_token))
            .context("the token service's answer has no token")?;
        *self.token.lock().unwrap() = Some(token);
        Ok(())
    }
}

/// `host` or `host:port` of `url`.
fn authority(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    }
}

/// The scheme and parameters of a challenge, like
/// `Bearer realm="https://ghcr.io/token",service="ghcr.io"`.
pub fn parse_challenge(challenge: &str) -> Option<(String, HashMap<String, String>)> {
    let challenge = challenge.trim();
    let (scheme, rest) = challenge.split_once(' ').unwrap_or((challenge, ""));
    let mut params = HashMap::new();
    let mut rest = rest.trim();
    while !rest.is_empty() {
        let (key, after) = rest.split_once('=')?;
        let (value, after) = match after.strip_prefix('"') {
            Some(quoted) => {
                let end = quoted.find('"')?;
                (&quoted[..end], &quoted[end + 1..])
            }
            None => after.split_once(',').unwrap_or((after, "")),
        };
        params.insert(key.trim().to_ascii_lowercase(), value.to_owned());
        rest = after.trim_start().trim_start_matches(',').trim_start();
    }
    Some((scheme.to_owned(), params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_challenges() {
        let (scheme, params) = parse_challenge(
            r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:a/b:pull""#,
        )
        .unwrap();
        assert_eq!(scheme, "Bearer");
        assert_eq!(params["realm"], "https://ghcr.io/token");
        assert_eq!(params["service"], "ghcr.io");
        assert_eq!(params["scope"], "repository:a/b:pull");
        let (_, params) = parse_challenge("bearer realm=https://r/t, service=s").unwrap();
        assert_eq!(
            (params["realm"].as_str(), params["service"].as_str()),
            ("https://r/t", "s")
        );
        let (scheme, params) = parse_challenge(r#"Basic realm="Registry Realm""#).unwrap();
        assert_eq!(
            (scheme.as_str(), params["realm"].as_str()),
            ("Basic", "Registry Realm")
        );
    }
}
