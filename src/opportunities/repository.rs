use chrono::{DateTime, Utc};
use firestore::*;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};

use super::types::{BookingDoc, OpeningHours, OpeningHoursDay, TimeBlockDoc};
use super::vacation_expander::expand_vacations_and_breaks;
use crate::AppState;

#[derive(Debug, thiserror::Error)]
pub enum OppRepoError {
    #[error("barber not found")]
    BarberNotFound,
    #[error("firestore: {0}")]
    Firestore(String),
}

pub struct ComputeInputs {
    pub bookings: Vec<BookingDoc>,
    pub opening_hours: OpeningHours,
    pub time_blocks: Vec<TimeBlockDoc>,
    pub user_names: HashMap<String, String>,
    pub user_photos: HashMap<String, String>,
    pub user_phones: HashMap<String, String>,
    pub timezone: chrono_tz::Tz,
}

pub async fn load_inputs_for_barber(
    state: &AppState,
    barber_id: &str,
    horizon_days: u32,
    now: DateTime<Utc>,
) -> Result<ComputeInputs, OppRepoError> {
    let (bookings, barber_raw, time_blocks) = tokio::try_join!(
        fetch_bookings(state, barber_id),
        fetch_barber_raw(state, barber_id),
        fetch_time_blocks(state, barber_id, now, horizon_days),
    )?;

    let Some(barber_raw) = barber_raw else {
        return Err(OppRepoError::BarberNotFound);
    };

    let opening_hours = parse_opening_hours(&barber_raw);
    let timezone = parse_timezone(&barber_raw);

    let tz_today = now.with_timezone(&timezone).date_naive();
    let horizon_end = tz_today + chrono::Duration::days(horizon_days as i64);
    let mut all_blocks = time_blocks;
    all_blocks.extend(expand_vacations_and_breaks(
        &barber_raw,
        barber_id,
        tz_today,
        horizon_end,
        timezone,
    ));

    let user_ids: Vec<String> = bookings
        .iter()
        .map(|b| b.user_id.clone())
        .filter(|u| !u.is_empty())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let user_docs = fetch_user_docs(state, &user_ids).await;

    let user_names = project_field(&user_docs, &["displayName", "name", "email"]);
    let user_photos = project_field(&user_docs, &["photoURL", "imageUrl", "photoUrl"]);
    let user_phones = project_field(&user_docs, &["phoneNumber", "phone", "phoneNum"]);

    Ok(ComputeInputs {
        bookings,
        opening_hours,
        time_blocks: all_blocks,
        user_names,
        user_photos,
        user_phones,
        timezone,
    })
}

async fn fetch_bookings(
    state: &AppState,
    barber_id: &str,
) -> Result<Vec<BookingDoc>, OppRepoError> {
    let stream: futures::stream::BoxStream<FirestoreResult<BookingDoc>> = state
        .db
        .fluent()
        .select()
        .from("bookings")
        .filter(|q| q.for_all([q.field("barberId").eq(barber_id)]))
        .obj()
        .stream_query_with_errors()
        .await
        .map_err(|e| OppRepoError::Firestore(e.to_string()))?;

    let docs: Vec<BookingDoc> = stream
        .filter_map(|res| async move { res.ok() })
        .collect()
        .await;

    Ok(docs)
}

async fn fetch_barber_raw(
    state: &AppState,
    barber_id: &str,
) -> Result<Option<serde_json::Value>, OppRepoError> {
    let doc: FirestoreResult<Option<serde_json::Value>> = state
        .db
        .fluent()
        .select()
        .by_id_in("barbers")
        .obj()
        .one(barber_id)
        .await;
    doc.map_err(|e| OppRepoError::Firestore(e.to_string()))
}

async fn fetch_time_blocks(
    state: &AppState,
    barber_id: &str,
    now: DateTime<Utc>,
    horizon_days: u32,
) -> Result<Vec<TimeBlockDoc>, OppRepoError> {
    let end = now + chrono::Duration::days(horizon_days as i64);
    let stream_res = state
        .db
        .fluent()
        .select()
        .from("time_blocks")
        .filter(|q| {
            q.for_all([
                q.field("barberId").eq(barber_id),
                q.field("startTime").greater_than_or_equal(now),
                q.field("startTime").less_than(end),
            ])
        })
        .obj::<TimeBlockDoc>()
        .stream_query_with_errors()
        .await;

    let stream = match stream_res {
        Ok(s) => s,
        Err(e) => {
            // Some Firestore projects reject range queries without the composite index;
            // fall back to barberId-only and filter in memory so first-run doesn't fail hard.
            tracing::warn!(
                error = %e,
                "time_blocks range query failed, falling back to barberId-only filter"
            );
            let fallback: futures::stream::BoxStream<FirestoreResult<TimeBlockDoc>> = state
                .db
                .fluent()
                .select()
                .from("time_blocks")
                .filter(|q| q.for_all([q.field("barberId").eq(barber_id)]))
                .obj()
                .stream_query_with_errors()
                .await
                .map_err(|e| OppRepoError::Firestore(e.to_string()))?;

            let all: Vec<TimeBlockDoc> = fallback
                .filter_map(|res| async move { res.ok() })
                .collect()
                .await;
            return Ok(all
                .into_iter()
                .filter(|b| {
                    b.start_time
                        .map(|s| s >= now && s < end)
                        .unwrap_or(false)
                })
                .collect());
        }
    };

    let docs: Vec<TimeBlockDoc> = stream
        .filter_map(|res| async move { res.ok() })
        .collect()
        .await;
    Ok(docs)
}

async fn fetch_user_docs(
    state: &AppState,
    user_ids: &[String],
) -> HashMap<String, serde_json::Value> {
    if user_ids.is_empty() {
        return HashMap::new();
    }

    let results = futures::stream::iter(user_ids.iter().cloned())
        .map(|uid| {
            let db = state.db.clone();
            async move {
                let res: FirestoreResult<Option<serde_json::Value>> =
                    db.fluent().select().by_id_in("users").obj().one(&uid).await;
                (uid, res)
            }
        })
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;

    let mut out = HashMap::new();
    for (uid, res) in results {
        match res {
            Ok(Some(doc)) => {
                out.insert(uid, doc);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(user_id = %uid, error = %e, "failed to load user doc");
            }
        }
    }
    out
}

fn project_field(
    docs: &HashMap<String, serde_json::Value>,
    aliases: &[&str],
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (uid, doc) in docs {
        for alias in aliases {
            if let Some(v) = doc.get(*alias).and_then(|v| v.as_str()) {
                if !v.is_empty() {
                    out.insert(uid.clone(), v.to_string());
                    break;
                }
            }
        }
    }
    out
}

fn parse_opening_hours(raw: &serde_json::Value) -> OpeningHours {
    let Some(obj) = raw.get("openingHours").and_then(|v| v.as_object()) else {
        return OpeningHours::new();
    };
    let mut out = OpeningHours::new();
    for (k, v) in obj {
        match serde_json::from_value::<OpeningHoursDay>(v.clone()) {
            Ok(day) => {
                out.insert(k.to_lowercase(), day);
            }
            Err(e) => {
                tracing::debug!(day = %k, error = %e, "skipping unparseable openingHours entry");
            }
        }
    }
    out
}

fn parse_timezone(raw: &serde_json::Value) -> chrono_tz::Tz {
    raw.get("timezone")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<chrono_tz::Tz>().ok())
        .unwrap_or(chrono_tz::Asia::Jerusalem)
}
