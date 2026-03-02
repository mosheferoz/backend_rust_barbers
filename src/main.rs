use axum::http::{header, HeaderValue, Method};
use axum::{
    middleware,
    routing::{delete, get, post},
    Router,
};
use dotenv::dotenv;
use firestore::*;
use std::{net::SocketAddr, str::FromStr};
use tower_http::cors::{Any, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod auth;
mod reminders;
mod team;

#[derive(Clone)]
pub struct AppState {
    pub db: FirestoreDb,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rust_backend=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let project_id = std::env::var("PROJECT_ID").expect("PROJECT_ID must be set");
    println!("Connecting to Firestore project: {}", project_id);

    let db = FirestoreDb::new(&project_id).await?;
    let state = AppState { db };

    // Start reminder scheduler (runs every 60 seconds, sends due SMS from scheduled_reminders)
    tokio::spawn(reminders::start_reminder_scheduler(state.clone()));

    // Protected routes (require authentication)
    let api_routes = Router::new()
        .route("/team/update-permissions", post(team::update_permissions))
        .route("/team/delete-member", post(team::delete_member))
        .route("/team/add-member", post(team::add_team_member))
        .route("/schedule-reminder", post(reminders::schedule_reminder))
        .route("/cancel-reminder/:id", delete(reminders::cancel_reminder))
        .route_layer(middleware::from_fn(auth::auth_middleware))
        .with_state(state);

    // Public routes
    let public_routes = Router::new().route("/", get(health_check));

    let cors = build_cors_layer_from_env();

    let app = Router::new()
        .merge(public_routes)
        .merge(api_routes)
        .layer(cors);

    let addr = bind_addr_from_env().unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8080)));
    println!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "Backend is running!"
}

fn bind_addr_from_env() -> Option<SocketAddr> {
    let raw = std::env::var("BIND_ADDR").ok()?;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    SocketAddr::from_str(raw).ok()
}

fn build_cors_layer_from_env() -> CorsLayer {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE, header::ACCEPT]);

    let origins = std::env::var("CORS_ALLOWED_ORIGINS").ok();
    match origins.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(list) => {
            let allowed: Vec<HeaderValue> = list
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .filter_map(|s| HeaderValue::from_str(s).ok())
                .collect();

            if allowed.is_empty() {
                cors.allow_origin(Any)
            } else {
                cors.allow_origin(allowed)
            }
        }
        None => cors.allow_origin(Any),
    }
}
