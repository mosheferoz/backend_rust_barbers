use chrono::Utc;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::algorithm::{compute_cadences, find_gaps, match_opportunities, CadenceParams, FindGapsParams};
use super::cache::{cap_horizons, CacheDoc, CacheStats, HorizonBuckets, SCHEMA_VERSION};
use super::repository::load_inputs_for_barber;
use crate::AppState;

#[derive(Debug, Clone, Default)]
pub struct SchedulerStats {
    pub last_tick_at: Option<chrono::DateTime<Utc>>,
    pub last_tick_duration_ms: u64,
    pub last_tick_barbers_processed: u32,
    pub last_tick_errors: u32,
}

pub type SharedStats = Arc<RwLock<SchedulerStats>>;

/// Supervised wrapper: restarts the scheduler on panic.
pub async fn start_opportunities_scheduler(state: AppState) {
    loop {
        let state = state.clone();
        let join = tokio::spawn(async move { scheduler_body(state).await });
        match join.await {
            Ok(()) => tracing::warn!("opportunities scheduler exited cleanly, restarting in 10s"),
            Err(e) if e.is_panic() => {
                tracing::error!(error = ?e, "opportunities scheduler panicked, restarting in 10s");
            }
            Err(e) => {
                tracing::error!(error = ?e, "opportunities scheduler join error, restarting in 10s");
            }
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn scheduler_body(state: AppState) {
    let interval_secs = env_u64("OPPORTUNITIES_INTERVAL_SECS", 3600);
    let concurrency = env_u64("OPPORTUNITIES_CONCURRENCY", 4) as usize;
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await; // skip initial immediate tick
    tracing::info!(interval_secs, concurrency, "opportunities scheduler started");

    loop {
        ticker.tick().await;
        let tick_start = std::time::Instant::now();
        let barber_ids = match enumerate_active_barbers(&state).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::error!(error = %e, "failed to enumerate barbers");
                let mut s = state.opp_stats.write().await;
                s.last_tick_at = Some(Utc::now());
                s.last_tick_duration_ms = tick_start.elapsed().as_millis() as u64;
                s.last_tick_errors = s.last_tick_errors.saturating_add(1);
                continue;
            }
        };
        let total = barber_ids.len();
        let results: Vec<(String, Result<CacheDoc, String>)> =
            futures::stream::iter(barber_ids)
                .map(|id| {
                    let state = state.clone();
                    async move {
                        let r = compute_and_write(&state, &id, "scheduler").await;
                        (id, r)
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

        let errors = results.iter().filter(|(_, r)| r.is_err()).count();
        for (id, r) in &results {
            if let Err(e) = r {
                tracing::error!(barber_id = %id, error = %e, "opportunities compute failed");
            }
        }

        let elapsed_ms = tick_start.elapsed().as_millis() as u64;
        tracing::info!(
            total,
            errors,
            elapsed_ms,
            "opportunities scheduler tick complete"
        );
        let mut s = state.opp_stats.write().await;
        s.last_tick_at = Some(Utc::now());
        s.last_tick_duration_ms = elapsed_ms;
        s.last_tick_barbers_processed = total as u32;
        s.last_tick_errors = errors as u32;
    }
}

pub async fn compute_and_write(
    state: &AppState,
    barber_id: &str,
    trigger: &str,
) -> Result<CacheDoc, String> {
    let compute_started_at = Utc::now();
    let now = compute_started_at;
    let inputs = load_inputs_for_barber(state, barber_id, 30, now)
        .await
        .map_err(|e| e.to_string())?;

    let mut buckets = HorizonBuckets::default();
    let cadences = compute_cadences(
        &inputs.bookings,
        &inputs.user_names,
        &inputs.user_photos,
        &inputs.user_phones,
        &CadenceParams {
            barber_id: barber_id.to_string(),
            due_factor: 1.0,
            min_visits_for_cadence: 2,
            max_overdue_days_to_show: 90,
            now,
            timezone: inputs.timezone,
        },
    );
    let cadences_count = cadences.len() as u32;

    for (h, bucket) in [
        (7u32, &mut buckets.h7),
        (14u32, &mut buckets.h14),
        (30u32, &mut buckets.h30),
    ] {
        let gaps = find_gaps(
            &inputs.opening_hours,
            &inputs.bookings,
            &inputs.time_blocks,
            &FindGapsParams {
                barber_id: barber_id.to_string(),
                horizon_days: h,
                slot_minutes: 30,
                now,
                timezone: inputs.timezone,
            },
        );
        *bucket = match_opportunities(cadences.clone(), gaps);
    }

    let elapsed_ms = (Utc::now() - compute_started_at).num_milliseconds().max(0) as u64;

    let mut doc = CacheDoc {
        schema_version: SCHEMA_VERSION,
        barber_id: barber_id.to_string(),
        computed_at: Utc::now().to_rfc3339(),
        computed_from_input_at: compute_started_at.to_rfc3339(),
        scheduler_run_id: Uuid::new_v4().to_string(),
        trigger: trigger.to_string(),
        horizons: buckets,
        stats: CacheStats {
            bookings_scanned: inputs.bookings.len() as u32,
            cadences_computed: cadences_count,
            elapsed_ms,
            firestore_reads: 0,
        },
        last_error: None,
    };
    cap_horizons(&mut doc, 100);
    super::cache::write(state, &doc).await?;
    Ok(doc)
}

async fn enumerate_active_barbers(state: &AppState) -> Result<Vec<String>, String> {
    let stream_res = state
        .db
        .fluent()
        .select()
        .from("barbers")
        .obj::<serde_json::Value>()
        .stream_query_with_errors()
        .await;

    let stream = stream_res.map_err(|e| e.to_string())?;
    let docs: Vec<serde_json::Value> = stream
        .filter_map(|res| async move { res.ok() })
        .collect()
        .await;

    let mut ids = Vec::with_capacity(docs.len());
    for doc in docs {
        // Skip obviously inactive entries.
        let status = doc.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if matches!(status, "deleted" | "suspended" | "inactive") {
            continue;
        }
        let id = doc
            .get("id")
            .or_else(|| doc.get("__name__"))
            .or_else(|| doc.get("uid"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if let Some(id) = id {
            if !id.is_empty() {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
