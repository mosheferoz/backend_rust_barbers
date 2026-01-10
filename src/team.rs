use crate::auth::Claims;
use crate::AppState;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use firestore::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct UpdatePermissionsRequest {
    #[serde(rename = "memberId")]
    pub member_id: String,
    pub permissions: HashMap<String, bool>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteMemberRequest {
    #[serde(rename = "memberId")]
    pub member_id: String,
}

#[derive(Debug, Deserialize)]
pub struct AddTeamMemberRequest {
    pub email: String,
    pub name: String,
    pub phone: String,
    pub role: String, // "manager" or "employee" (comes as "מנהל" or "עובד" from client)
    pub permissions: HashMap<String, bool>,
    #[serde(rename = "revenueShare")]
    pub revenue_share: Option<String>,
    pub bio: Option<String>,
}

#[derive(Serialize)]
pub struct GenericResponse {
    pub success: bool,
    pub message: Option<String>,
}

// Helper to check if user can manage team
async fn check_manage_team_permission(claims: &Claims, state: &AppState) -> bool {
    // 1. Check custom claim (set by admin or Function)
    if claims.has_permission("manageTeam") {
        return true;
    }

    // 2. Check if user is owner in their barber profile
    let uid = claims.uid();

    // Check if there is a barber profile with this ID (if they are a barber)
    let barber_profile: FirestoreResult<Option<serde_json::Value>> = state
        .db
        .fluent()
        .select()
        .by_id_in("barbers")
        .obj()
        .one(uid)
        .await;

    if let Ok(Some(profile)) = barber_profile {
        // Check status or role
        if let Some(status) = profile.get("status").and_then(|v| v.as_str()) {
            if status == "owner" {
                return true;
            }
        }
        if let Some(role) = profile.get("role").and_then(|v| v.as_str()) {
            if role == "owner" || role == "admin" || role == "manager" {
                // Managers also can manage team
                return true;
            }
        }
    }

    false
}

pub async fn update_permissions(
    claims: Claims,
    State(state): State<AppState>,
    Json(payload): Json<UpdatePermissionsRequest>,
) -> impl IntoResponse {
    if !check_manage_team_permission(&claims, &state).await {
        return (
            StatusCode::FORBIDDEN,
            Json(GenericResponse {
                success: false,
                message: Some("Permission denied: requires manageTeam".to_string()),
            }),
        )
            .into_response();
    }

    const COLLECTION_NAME: &str = "barbers";

    let doc_result: FirestoreResult<Option<serde_json::Value>> = state
        .db
        .fluent()
        .select()
        .by_id_in(COLLECTION_NAME)
        .obj()
        .one(&payload.member_id)
        .await;

    match doc_result {
        Ok(Some(mut doc)) => {
            if let Some(obj) = doc.as_object_mut() {
                obj.insert(
                    "permissions".to_string(),
                    serde_json::to_value(&payload.permissions).unwrap(),
                );
            }

            let update_result = state
                .db
                .fluent()
                .update()
                .in_col(COLLECTION_NAME)
                .document_id(&payload.member_id)
                .object(&doc)
                .execute::<()>()
                .await;

            match update_result {
                Ok(_) => Json(GenericResponse {
                    success: true,
                    message: None,
                })
                .into_response(),
                Err(e) => {
                    eprintln!("Error saving updated permissions: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(GenericResponse {
                            success: false,
                            message: Some(e.to_string()),
                        }),
                    )
                        .into_response()
                }
            }
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(GenericResponse {
                success: false,
                message: Some("Member not found".to_string()),
            }),
        )
            .into_response(),
        Err(e) => {
            eprintln!("Error fetching member: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GenericResponse {
                    success: false,
                    message: Some(e.to_string()),
                }),
            )
                .into_response()
        }
    }
}

pub async fn delete_member(
    claims: Claims,
    State(state): State<AppState>,
    Json(payload): Json<DeleteMemberRequest>,
) -> impl IntoResponse {
    if !check_manage_team_permission(&claims, &state).await {
        return (
            StatusCode::FORBIDDEN,
            Json(GenericResponse {
                success: false,
                message: Some("Permission denied: requires manageTeam".to_string()),
            }),
        )
            .into_response();
    }

    const COLLECTION_NAME: &str = "barbers";

    let result = state
        .db
        .fluent()
        .delete()
        .from(COLLECTION_NAME)
        .document_id(&payload.member_id)
        .execute()
        .await;

    match result {
        Ok(_) => Json(GenericResponse {
            success: true,
            message: None,
        })
        .into_response(),
        Err(e) => {
            eprintln!("Error deleting member: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GenericResponse {
                    success: false,
                    message: Some(e.to_string()),
                }),
            )
                .into_response()
        }
    }
}

pub async fn add_team_member(
    claims: Claims,
    State(state): State<AppState>,
    Json(payload): Json<AddTeamMemberRequest>,
) -> impl IntoResponse {
    // Check permission with fallback
    if !check_manage_team_permission(&claims, &state).await {
        return (
            StatusCode::FORBIDDEN,
            Json(GenericResponse {
                success: false,
                message: Some("Permission denied: requires manageTeam".to_string()),
            }),
        )
            .into_response();
    }

    // Lookup user by email in 'users' collection using simple filter
    let users_stream: futures::stream::BoxStream<FirestoreResult<serde_json::Value>> = state
        .db
        .fluent()
        .select()
        .from("users")
        .filter(|q| q.for_all([q.field("email").eq(payload.email.clone())]))
        .obj()
        .stream_query_with_errors()
        .await
        .unwrap();

    use futures::stream::StreamExt;
    let mut users: Vec<serde_json::Value> = users_stream
        .filter_map(|res| async move { res.ok() })
        .collect()
        .await;

    let user_option = users.pop();

    let new_member_id = if let Some(user) = &user_option {
        user.get("uid")
            .or(user.get("userId"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
    } else {
        uuid::Uuid::new_v4().to_string()
    };

    // Create BarberStruct-like object
    let mut barber_profile = serde_json::Map::new();
    barber_profile.insert(
        "id".to_string(),
        serde_json::Value::String(new_member_id.clone()),
    );
    barber_profile.insert("name".to_string(), serde_json::Value::String(payload.name));
    barber_profile.insert(
        "email".to_string(),
        serde_json::Value::String(payload.email),
    );
    barber_profile.insert(
        "phone".to_string(),
        serde_json::Value::String(payload.phone),
    );
    // For barber profile, we keep the Hebrew role as it is displayed in UI
    barber_profile.insert(
        "role".to_string(),
        serde_json::Value::String(payload.role.clone()),
    );
    barber_profile.insert(
        "permissions".to_string(),
        serde_json::to_value(&payload.permissions).unwrap(),
    );
    barber_profile.insert(
        "ownerId".to_string(),
        serde_json::Value::String(claims.uid().to_string()),
    );
    barber_profile.insert(
        "status".to_string(),
        serde_json::Value::String("active".to_string()),
    );

    if let Some(desc) = payload.bio {
        barber_profile.insert("description".to_string(), serde_json::Value::String(desc));
    }
    if let Some(rev) = payload.revenue_share {
        barber_profile.insert("revenueShare".to_string(), serde_json::Value::String(rev));
    }

    // Save to 'barbers' collection
    let save_result = state
        .db
        .fluent()
        .update()
        .in_col("barbers")
        .document_id(&new_member_id)
        .object(&serde_json::Value::Object(barber_profile))
        .execute::<()>()
        .await;

    if let Err(e) = save_result {
        eprintln!("Error saving barber profile: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(GenericResponse {
                success: false,
                message: Some(e.to_string()),
            }),
        )
            .into_response();
    }

    if let Some(_user) = user_option {
        // Map role to English for system/claims usage
        let system_role = match payload.role.as_str() {
            "מנהל" => "manager",
            "עובד" => "employee",
            _other => "employee", // Default fallback
        };

        let mut user_update = serde_json::Map::new();
        user_update.insert(
            "type".to_string(),
            serde_json::Value::String("employee".to_string()),
        );
        // Update user role as requested
        user_update.insert(
            "role".to_string(),
            serde_json::Value::String(system_role.to_string()),
        );
        user_update.insert(
            "barberProfileId".to_string(),
            serde_json::Value::String(new_member_id.clone()),
        );
        user_update.insert(
            "barberStatus".to_string(),
            serde_json::Value::String("active".to_string()),
        );
        user_update.insert(
            "ownerId".to_string(),
            serde_json::Value::String(claims.uid().to_string()),
        );

        let user_update_result = state
            .db
            .fluent()
            .update()
            .in_col("users")
            .document_id(&new_member_id)
            .object(&serde_json::Value::Object(user_update))
            .execute::<()>()
            .await;

        if let Err(e) = user_update_result {
            eprintln!("Error linking user profile: {}", e);
        }
    }

    Json(GenericResponse {
        success: true,
        message: None,
    })
    .into_response()
}
