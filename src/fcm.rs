//! Send push notifications via Firebase Cloud Messaging HTTP v1 API.

use reqwest::Client;
use serde::Serialize;

const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

#[derive(Serialize)]
struct FcmNotification {
    title: String,
    body: String,
}

/// Android-specific options. HIGH priority ensures delivery when app is in background or terminated.
#[derive(Serialize)]
struct FcmAndroidConfig {
    #[serde(rename = "priority")]
    priority: String,
}

#[derive(Serialize)]
struct FcmMessage {
    token: String,
    notification: FcmNotification,
    /// Ensures Android shows notification when app is closed / dozing
    #[serde(skip_serializing_if = "Option::is_none")]
    android: Option<FcmAndroidConfig>,
}

#[derive(Serialize)]
struct FcmSendRequest {
    message: FcmMessage,
}

/// Obtain a Bearer token for FCM using application default credentials.
async fn get_fcm_token() -> Result<String, String> {
    let provider = gcp_auth::provider().await.map_err(|e| e.to_string())?;
    let token = provider
        .token(&[FCM_SCOPE])
        .await
        .map_err(|e| e.to_string())?;
    Ok(token.as_str().to_string())
}

/// Send a push notification via FCM HTTP v1 API.
/// Returns the message ID on success.
pub async fn send_fcm_push(
    project_id: &str,
    fcm_token: &str,
    title: &str,
    body: &str,
) -> Result<String, String> {
    if fcm_token.is_empty() {
        return Err("FCM token is empty".to_string());
    }

    let access_token = get_fcm_token().await?;
    let url = format!(
        "https://fcm.googleapis.com/v1/projects/{}/messages:send",
        project_id
    );

    let payload = FcmSendRequest {
        message: FcmMessage {
            token: fcm_token.to_string(),
            notification: FcmNotification {
                title: title.to_string(),
                body: body.to_string(),
            },
            android: Some(FcmAndroidConfig {
                priority: "high".to_string(),
            }),
        },
    };

    let client = Client::new();
    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(format!("FCM HTTP {}: {}", status.as_u16(), text));
    }

    // Response body may contain "name": "projects/.../messages/..." - extract message id if needed
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
        if let Some(name) = json.get("name").and_then(|v| v.as_str()) {
            return Ok(name.to_string());
        }
    }
    Ok("sent".to_string())
}
