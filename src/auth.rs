use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    middleware::Next,
    response::Response,
};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// Google's public keys URL for Firebase Auth
const GOOGLE_PUBLIC_KEYS_URL: &str =
    "https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub aud: String,
    pub exp: usize,
    pub iat: usize,
    pub iss: String,
    pub sub: String,
    // uid is not standard in Firebase ID token payload (it's sub), removing it to fix error.
    // We'll alias uid to sub in logic if needed or just use sub.
    // However, if we want to use 'uid' field name in our struct for clarity, we can use serde alias if 'sub' is the source.
    // But 'sub' is the field name in JWT. So let's just use 'sub'.
    #[serde(alias = "user_id")]
    pub user_id: Option<String>, // Sometimes present
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Claims {
    // Helper to get user ID consistently
    pub fn uid(&self) -> &str {
        &self.sub
    }
}

// Global cache for public keys (simplistic implementation)
// In a production app, use a proper background task to refresh keys.
lazy_static::lazy_static! {
    static ref PUBLIC_KEYS: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(HashMap::new()));
}

async fn fetch_google_public_keys() -> Result<HashMap<String, String>, String> {
    let client = Client::new();
    let resp = client
        .get(GOOGLE_PUBLIC_KEYS_URL)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let keys = resp
        .json::<HashMap<String, String>>()
        .await
        .map_err(|e| e.to_string())?;
    Ok(keys)
}

async fn get_decoding_key(kid: &str) -> Result<DecodingKey, String> {
    // Check cache first
    {
        let keys = PUBLIC_KEYS.read().unwrap();
        if let Some(cert) = keys.get(kid) {
            return DecodingKey::from_rsa_pem(cert.as_bytes()).map_err(|e| e.to_string());
        }
    }

    // Refresh cache
    let new_keys = fetch_google_public_keys().await?;
    {
        let mut keys = PUBLIC_KEYS.write().unwrap();
        *keys = new_keys.clone();
    }

    if let Some(cert) = new_keys.get(kid) {
        DecodingKey::from_rsa_pem(cert.as_bytes()).map_err(|e| e.to_string())
    } else {
        Err("Key ID not found".to_string())
    }
}

pub async fn auth_middleware(
    request: axum::extract::Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let (mut parts, body) = request.into_parts();

    // Extract Bearer token manually
    let auth_header = parts
        .headers
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !auth_header.starts_with("Bearer ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = &auth_header[7..];

    // Decode header to get kid
    let header = decode_header(token).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let kid = header.kid.ok_or(StatusCode::UNAUTHORIZED)?;

    // Get public key
    let decoding_key = get_decoding_key(&kid).await.map_err(|e| {
        eprintln!("Error getting public key: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Validate token
    let project_id = std::env::var("PROJECT_ID").unwrap_or_default();
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[project_id]);

    let token_data = decode::<Claims>(token, &decoding_key, &validation).map_err(|e| {
        eprintln!("Token validation failed: {}", e);
        StatusCode::UNAUTHORIZED
    })?;

    // Inject claims into request
    parts.extensions.insert(token_data.claims);

    let request = axum::extract::Request::from_parts(parts, body);
    Ok(next.run(request).await)
}

// Extractor for Claims
#[async_trait]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Claims>()
            .cloned()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

// Helper to check permissions
impl Claims {
    pub fn has_permission(&self, permission: &str) -> bool {
        if let Some(val) = self.extra.get(permission) {
            val.as_bool().unwrap_or(false)
        } else {
            false
        }
    }
}
