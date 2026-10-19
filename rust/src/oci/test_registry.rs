//! A registry for the tests, with just enough of the OCI distribution API to
//! pull: manifests and blobs by digest or tag, and anonymous tokens from its
//! token service.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

const TOKEN: &str = "granted-token";

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Login {
    /// Anyone may pull.
    None,
    /// Every request needs a token from the token service, which gives anyone
    /// one, as for public repositories on ghcr.io.
    Token,
}

/// Content by path, with its content type.
pub type Files = HashMap<String, (String, Vec<u8>)>;

#[derive(Clone)]
pub struct Registry {
    /// `127.0.0.1:<port>`.
    pub host: String,
    /// What it holds by path, like `/v2/<name>/blobs/<digest>`, with each
    /// manifest's content type.
    pub files: Arc<Mutex<Files>>,
    /// Each request as `<method> <path>`, without the query.
    pub log: Arc<Mutex<Vec<String>>>,
}

impl Registry {
    pub async fn start(login: Login) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let registry = Self {
            host: listener.local_addr().unwrap().to_string(),
            files: Arc::default(),
            log: Arc::default(),
        };
        let this = registry.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let this = this.clone();
                let service = service_fn(move |request| {
                    let this = this.clone();
                    async move { Ok::<_, Infallible>(this.respond(&request, login)) }
                });
                tokio::spawn(http1::Builder::new().serve_connection(TokioIo::new(stream), service));
            }
        });
        registry
    }

    /// The requests so far whose path contains `part`.
    pub fn requests(&self, part: &str) -> Vec<String> {
        let log = self.log.lock().unwrap();
        log.iter().filter(|l| l.contains(part)).cloned().collect()
    }

    pub fn put(&self, path: String, content_type: &str, body: Vec<u8>) {
        (self.files.lock().unwrap()).insert(path, (content_type.to_owned(), body));
    }

    pub fn get(&self, path: &str) -> Option<Vec<u8>> {
        self.files.lock().unwrap().get(path).map(|(_, b)| b.clone())
    }

    /// What to answer a request with `authorization` if it may not go on.
    fn challenge(&self, login: Login, authorization: &str) -> Option<String> {
        match login {
            Login::None => None,
            Login::Token if authorization == format!("Bearer {TOKEN}") => None,
            Login::Token => Some(format!(
                r#"Bearer realm="http://{}/token",service="test""#,
                self.host
            )),
        }
    }

    fn respond(&self, request: &Request<Incoming>, login: Login) -> Response<Full<Bytes>> {
        let path = request.uri().path().to_owned();
        let query = request.uri().query().unwrap_or_default().to_owned();
        let method = request.method().clone();
        (self.log.lock().unwrap()).push(format!("{method} {path}"));
        let authorization = (request.headers().get(header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let reply = |status: StatusCode, body: Vec<u8>| {
            let mut response = Response::new(Full::new(Bytes::from(body)));
            *response.status_mut() = status;
            response
        };
        if path == "/token" {
            if login != Login::Token {
                return reply(StatusCode::UNAUTHORIZED, Vec::new());
            }
            assert!(query.contains("scope=repository%3A"), "{query}");
            return reply(
                StatusCode::OK,
                format!(r#"{{"token":"{TOKEN}"}}"#).into_bytes(),
            );
        }
        let challenge = self.challenge(login, &authorization);
        if let Some(challenge) = challenge {
            let mut response = reply(StatusCode::UNAUTHORIZED, b"{\"errors\":[]}".to_vec());
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, challenge.parse().unwrap());
            return response;
        }
        match method {
            Method::GET | Method::HEAD => match self.files.lock().unwrap().get(&path) {
                Some((content_type, content)) => {
                    let mut response = reply(StatusCode::OK, content.clone());
                    if method == Method::HEAD {
                        *response.body_mut() = Full::new(Bytes::new());
                    }
                    response
                        .headers_mut()
                        .insert(header::CONTENT_TYPE, content_type.parse().unwrap());
                    response
                }
                None => reply(StatusCode::NOT_FOUND, Vec::new()),
            },
            _ => reply(StatusCode::METHOD_NOT_ALLOWED, Vec::new()),
        }
    }
}
