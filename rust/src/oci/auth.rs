//! Credentials for registries, from a Docker config file like the one in a
//! `kubernetes.io/dockerconfigjson` Secret, and tokens from a registry's
//! token service.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, bail, ensure};
use base64::Engine;
use serde::Deserialize;
use url::Url;

use crate::transport;

/// Far more than a token service's answer needs.
const MAX_TOKEN: usize = 1 << 20;

/// The credentials for one repository.
pub struct Credentials {
    /// A Docker config file. Each request reads it, so an updated Secret takes
    /// effect immediately.
    file: Option<PathBuf>,
    /// `host` or `host:port`, the key in the file.
    host: String,
    /// What the token service grants, like `repository:<name>:pull`.
    scope: String,
    /// The token service's last token.
    token: Mutex<Option<String>>,
    /// Hosts whose token service may be plain HTTP.
    insecure: Vec<String>,
}

#[derive(Deserialize)]
struct DockerConfig {
    #[serde(default)]
    auths: HashMap<String, DockerAuth>,
}

#[derive(Deserialize)]
struct DockerAuth {
    /// Base64 of `<username>:<password>`.
    auth: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

/// What a token service answers. Docker's registry names the token `token`,
/// and OAuth 2 names it `access_token`.
#[derive(Deserialize)]
struct TokenAnswer {
    token: Option<String>,
    access_token: Option<String>,
}

impl Credentials {
    pub fn new(file: Option<PathBuf>, host: &str, scope: String, insecure: Vec<String>) -> Self {
        Self {
            file,
            host: host.to_owned(),
            scope,
            token: Mutex::default(),
            insecure,
        }
    }

    /// Adds the token, or else the login for the host, to a request for
    /// `url`. A request to another host, like a redirect to blob storage,
    /// gets neither.
    pub fn authorize(
        &self,
        request: reqwest::RequestBuilder,
        url: &Url,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        if authority(url) != self.host {
            return Ok(request);
        }
        if let Some(token) = &*self.token.lock().unwrap() {
            return Ok(request.bearer_auth(token));
        }
        Ok(match self.login()? {
            Some((user, password)) => request.basic_auth(user, Some(password)),
            None => request,
        })
    }

    /// The user and password for the host in the file, if any.
    fn login(&self) -> anyhow::Result<Option<(String, String)>> {
        match &self.file {
            Some(file) => login(file, &self.host),
            None => Ok(None),
        }
    }

    /// Answers a 401's `WWW-Authenticate`. For `Bearer`, it gets a token from
    /// the service the challenge names, with the login if there is one, and
    /// requests use it from then on. `Basic` asks for a login, which a
    /// request already sends if there is one.
    pub async fn answer(&self, challenge: &str) -> anyhow::Result<()> {
        let (scheme, params) = parse_challenge(challenge)
            .with_context(|| format!("unsupported WWW-Authenticate {challenge:?}"))?;
        let login = self.login()?;
        if scheme.eq_ignore_ascii_case("basic") {
            match login {
                Some(_) => bail!("the registry refused the login for {}", self.host),
                None => bail!("the registry wants a login for {}", self.host),
            }
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
            "the token service {realm} isn't https://"
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
        let mut request = transport::client()?.get(url.clone());
        if let Some((user, password)) = &login {
            request = request.basic_auth(user, Some(password));
        }
        let resp = request.send().await.map_err(reqwest::Error::without_url)?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            match login {
                Some(_) => bail!("the token service refused the login for {}", self.host),
                None => bail!("the token service wants a login for {}", self.host),
            }
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

/// The login for `host` in the Docker config `file`. The file's keys may be
/// URLs, like Docker Hub's `https://index.docker.io/v1/`. Errors don't quote
/// the file, which holds passwords.
pub fn login(file: &Path, host: &str) -> anyhow::Result<Option<(String, String)>> {
    let text = std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
    let config: DockerConfig = serde_json::from_slice(&text)
        .map_err(|_| anyhow::anyhow!("{} isn't a Docker config", file.display()))?;
    let hub = |h: &str| matches!(h, "docker.io" | "index.docker.io" | "registry-1.docker.io");
    let entry = (config.auths.iter()).find(|(key, _)| {
        let key = key
            .strip_prefix("https://")
            .or(key.strip_prefix("http://"))
            .unwrap_or(key);
        let key = key.split('/').next().unwrap_or_default();
        key.eq_ignore_ascii_case(host) || (hub(key) && hub(host))
    });
    let Some((_, auth)) = entry else {
        return Ok(None);
    };
    if let (Some(user), Some(password)) = (&auth.username, &auth.password) {
        return Ok(Some((user.clone(), password.clone())));
    }
    let Some(encoded) = &auth.auth else {
        return Ok(None);
    };
    let bad = || anyhow::anyhow!("{} has a bad auth for {host}", file.display());
    let decoded =
        (base64::engine::general_purpose::STANDARD.decode(encoded.trim())).map_err(|_| bad())?;
    let decoded = String::from_utf8(decoded).map_err(|_| bad())?;
    let (user, password) = decoded.split_once(':').ok_or_else(bad)?;
    Ok(Some((user.to_owned(), password.to_owned())))
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

    #[test]
    fn reads_docker_configs() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.json");
        let auth = base64::engine::general_purpose::STANDARD.encode("ann:s3cret:x");
        std::fs::write(
            &file,
            format!(
                r#"{{"auths": {{
                    "registry.local:5000": {{"auth": "{auth}"}},
                    "https://index.docker.io/v1/": {{"username": "bob", "password": "pw"}},
                    "https://other.example/v2/": {{"auth": "{auth}"}},
                    "broken.example": {{"auth": "%%%"}}
                }}, "credsStore": "desktop"}}"#
            ),
        )
        .unwrap();
        let login = |host| login(&file, host).map(|l| l.map(|(u, p)| format!("{u}:{p}")));
        // The password may hold a colon.
        assert_eq!(
            login("registry.local:5000").unwrap().unwrap(),
            "ann:s3cret:x"
        );
        assert_eq!(
            login("REGISTRY.local:5000").unwrap().unwrap(),
            "ann:s3cret:x"
        );
        assert_eq!(login("registry.local").unwrap(), None);
        assert_eq!(login("registry-1.docker.io").unwrap().unwrap(), "bob:pw");
        assert_eq!(login("other.example").unwrap().unwrap(), "ann:s3cret:x");
        let e = format!("{:#}", login("broken.example").unwrap_err());
        assert!(e.contains("bad auth for broken.example"), "{e}");

        // An error doesn't quote the file.
        std::fs::write(&file, r#"{"auths": {"x": {"auth": "s3cret""#).unwrap();
        let e = format!("{:#}", login("x").unwrap_err());
        assert!(
            e.contains("isn't a Docker config") && !e.contains("s3cret"),
            "{e}"
        );
    }
}
