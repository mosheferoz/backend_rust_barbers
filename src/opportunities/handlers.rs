use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::auth::Claims;
use crate::AppState;

use super::cache;
use super::scheduler::compute_and_write;

const RECOMPUTE_COOLDOWN_SECS: u64 = 30;

lazy_static::lazy_static! {
    /// Simple per-(uid, barber_id) cooldown tracker. Keys expire after the
    /// cooldown window on next access, so the map stays bounded in practice.
    static ref RATE_LIMITER: Mutex<HashMap<(String, String), Instant>> =
        Mutex::new(HashMap::new());
}

fn check_rate_limit(uid: &str, barber_id: &str) -> bool {
    let key = (uid.to_string(), barber_id.to_string());
    let now = Instant::now();
    let window = Duration::from_secs(RECOMPUTE_COOLDOWN_SECS);
    let mut map = match RATE_LIMITER.lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    map.retain(|_, t| now.duration_since(*t) < window);
    if map.contains_key(&key) {
        return false;
    }
    map.insert(key, now);
    true
}

#[derive(Deserialize)]
pub struct RecomputeReq {
    #[serde(rename = "barberId")]
    pub barber_id: String,
}

fn is_not_true(b: &bool) -> bool {
    !*b
}

#[derive(Serialize)]
pub struct RecomputeResp {
    pub success: bool,
    #[serde(rename = "computedAt")]
    pub computed_at: String,
    #[serde(rename = "fromCache", skip_serializing_if = "is_not_true")]
    pub from_cache: bool,
}

pub async fn recompute(
    claims: Claims,
    State(state): State<AppState>,
    Json(req): Json<RecomputeReq>,
) -> impl IntoResponse {
    if !can_recompute_for(&claims, &state, &req.barber_id).await {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"success": false, "message": "forbidden"})),
        )
            .into_response();
    }

    if !check_rate_limit(claims.uid(), &req.barber_id) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"success": false, "message": "rate_limited"})),
        )
            .into_response();
    }

    // Coalesce: if the cache was refreshed in the last 30 s, return it.
    if let Ok(Some(existing)) = cache::read(&state, &req.barber_id).await {
        if let Ok(computed) = chrono::DateTime::parse_from_rfc3339(&existing.computed_at) {
            let age =
                chrono::Utc::now().signed_duration_since(computed.with_timezone(&chrono::Utc));
            if age < chrono::Duration::seconds(RECOMPUTE_COOLDOWN_SECS as i64) {
                return Json(RecomputeResp {
                    success: true,
                    computed_at: existing.computed_at,
                    from_cache: true,
                })
                .into_response();
            }
        }
    }

    match compute_and_write(&state, &req.barber_id, "invalidation").await {
        Ok(doc) => Json(RecomputeResp {
            success: true,
            computed_at: doc.computed_at,
            from_cache: false,
        })
        .into_response(),
        Err(e) => {
            tracing::error!(barber_id = %req.barber_id, error = %e, "recompute failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"success": false, "message": e})),
            )
                .into_response()
        }
    }
}

async fn can_recompute_for(claims: &Claims, state: &AppState, target: &str) -> bool {
    if claims.uid() == target {
        return true;
    }
    if claims.has_permission("manageOpportunities") {
        return true;
    }
    crate::team::check_manage_team_permission(claims, state).await
}

#[derive(Deserialize)]
pub struct StatusQuery {
    #[serde(rename = "barberId")]
    pub barber_id: String,
}

#[derive(Serialize)]
pub struct StatusResp {
    #[serde(rename = "computedAt", skip_serializing_if = "Option::is_none")]
    pub computed_at: Option<String>,
    #[serde(rename = "matchedCount")]
    pub matched_count: u32,
    #[serde(rename = "dueWithoutGapCount")]
    pub due_without_gap_count: u32,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

pub async fn status(
    claims: Claims,
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<StatusQuery>,
) -> impl IntoResponse {
    if !can_recompute_for(&claims, &state, &q.barber_id).await {
        return (StatusCode::FORBIDDEN, Json(json!({}))).into_response();
    }
    match cache::read(&state, &q.barber_id).await {
        Ok(Some(doc)) => Json(StatusResp {
            computed_at: Some(doc.computed_at),
            matched_count: doc.horizons.h14.matched.len() as u32,
            due_without_gap_count: doc.horizons.h14.due_without_gap.len() as u32,
            last_error: doc.last_error,
        })
        .into_response(),
        Ok(None) => Json(StatusResp {
            computed_at: None,
            matched_count: 0,
            due_without_gap_count: 0,
            last_error: None,
        })
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

#[derive(Serialize)]
pub struct HealthResp {
    pub ok: bool,
    #[serde(rename = "schedulerLastTickAt", skip_serializing_if = "Option::is_none")]
    pub scheduler_last_tick_at: Option<String>,
    #[serde(rename = "schedulerLastTickDurationMs")]
    pub scheduler_last_tick_duration_ms: u64,
    #[serde(rename = "schedulerLastTickBarbersProcessed")]
    pub scheduler_last_tick_barbers_processed: u32,
    #[serde(rename = "schedulerLastTickErrors")]
    pub scheduler_last_tick_errors: u32,
}

pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.opp_stats.read().await;
    Json(HealthResp {
        ok: true,
        scheduler_last_tick_at: s.last_tick_at.map(|t| t.to_rfc3339()),
        scheduler_last_tick_duration_ms: s.last_tick_duration_ms,
        scheduler_last_tick_barbers_processed: s.last_tick_barbers_processed,
        scheduler_last_tick_errors: s.last_tick_errors,
    })
}
