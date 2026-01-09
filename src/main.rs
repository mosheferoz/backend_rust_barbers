use axum::{
    extract::{State, Path},
    routing::{get, post, delete},
    Json, Router,
    middleware,
};
use axum::http::{header, HeaderValue, Method};
use firestore::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, str::FromStr};
use dotenv::dotenv;
use tower_http::cors::{CorsLayer, Any};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod team;
mod reminders;
mod auth;

// מבנה נתונים לדוגמה - ספר
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Barber {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    name: String,
    email: Option<String>,
    #[serde(default)]
    phone: String,
    // הוסף שדות נוספים בהתאם לצורך
}

// מצב האפליקציה המשותף
#[derive(Clone)]
pub struct AppState {
    pub db: FirestoreDb,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // טעינת משתני סביבה
    dotenv().ok();

    // אתחול מערכת הלוגים
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rust_backend=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // התקנת ספק קריפטו כנדרש בתיעוד
    // rustls::crypto::ring::default_provider().install_default().ok(); // Un-comment if needed explicitly

    let project_id = std::env::var("PROJECT_ID").expect("PROJECT_ID must be set");
    println!("Connecting to Firestore project: {}", project_id);

    // יצירת קלינט לפיירסטור
    let db = FirestoreDb::new(&project_id).await?;

    let state = AppState { db };

    // Routes that require authentication and permission checks
    let api_routes = Router::new()
        .route("/team/update-permissions", post(team::update_permissions))
        .route("/team/delete-member", post(team::delete_member))
        .route("/team/add-member", post(team::add_team_member))
        .route("/schedule-reminder", post(reminders::schedule_reminder))
        .route("/cancel-reminder/:id", delete(reminders::cancel_reminder))
        .route_layer(middleware::from_fn(auth::auth_middleware))
        .with_state(state.clone());

    // Public routes
    let public_routes = Router::new()
        .route("/", get(health_check))
        .route("/barbers", get(list_barbers).post(create_barber))
        .route("/barbers/:id", get(get_barber))
        .with_state(state);

    // Merge routes
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

// שליפת רשימת ספרים
async fn list_barbers(State(state): State<AppState>) -> Json<Vec<Barber>> {
    const COLLECTION_NAME: &str = "barbers";
    
    // שימוש ב-Fluent API כפי שהתבקש
    let barbers_stream: futures::stream::BoxStream<FirestoreResult<Barber>> = state.db.fluent()
        .select()
        .from(COLLECTION_NAME)
        .obj()
        .stream_query_with_errors()
        .await
        .unwrap(); // בטיפול אמיתי נרצה לטפל בשגיאות בצורה יפה יותר

    use futures::stream::StreamExt;
    let barbers: Vec<Barber> = barbers_stream
        .filter_map(|res| async move { res.ok() })
        .collect()
        .await;

    Json(barbers)
}

// יצירת ספר חדש
async fn create_barber(
    State(state): State<AppState>,
    Json(payload): Json<Barber>,
) -> Json<Barber> {
    const COLLECTION_NAME: &str = "barbers";

    // אם אין ID, ניצור אחד או ניתן לפיירסטור ליצור (כאן אנחנו מצפים ל-ID או יוצרים רנדומלי אם נרצה)
    // בדוגמה זו נשתמש ב-update כדי ליצור או לעדכן, או insert אם יש ID
    
    let document_id = payload.id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    
    let _saved_barber: Barber = state.db.fluent()
        .insert()
        .into(COLLECTION_NAME)
        .document_id(&document_id)
        .object(&payload)
        .execute()
        .await
        .unwrap();

    // מחזירים את האובייקט (בפועל היינו רוצים להחזיר את מה שנשמר)
    Json(payload)
}

// קבלת ספר לפי מזהה
async fn get_barber(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Json<Option<Barber>> {
    const COLLECTION_NAME: &str = "barbers";

    let barber: Option<Barber> = state.db.fluent()
        .select()
        .by_id_in(COLLECTION_NAME)
        .obj()
        .one(&id)
        .await
        .unwrap();

    Json(barber)
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
    // Defaults are permissive to avoid breaking local/dev.
    // In production set CORS_ALLOWED_ORIGINS to a comma-separated list of exact origins:
    // e.g. "https://eventlyil.xyz,https://www.eventlyil.xyz,http://localhost:5173"
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
