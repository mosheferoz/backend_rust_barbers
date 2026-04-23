use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::types::MatchOpportunitiesResult;
use crate::AppState;

pub const SCHEMA_VERSION: u32 = 1;
const CACHE_COLLECTION: &str = "opportunities";
const CACHE_DOC_ID: &str = "cache";
const PARENT_COLLECTION: &str = "barbers";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheDoc {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,
    #[serde(rename = "barberId")]
    pub barber_id: String,
    #[serde(rename = "computedAt")]
    pub computed_at: String,
    #[serde(rename = "computedFromInputAt")]
    pub computed_from_input_at: String,
    #[serde(rename = "schedulerRunId")]
    pub scheduler_run_id: String,
    pub trigger: String, // "scheduler" | "invalidation"
    pub horizons: HorizonBuckets,
    pub stats: CacheStats,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HorizonBuckets {
    pub h7: MatchOpportunitiesResult,
    pub h14: MatchOpportunitiesResult,
    pub h30: MatchOpportunitiesResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CacheStats {
    #[serde(rename = "bookingsScanned")]
    pub bookings_scanned: u32,
    #[serde(rename = "cadencesComputed")]
    pub cadences_computed: u32,
    #[serde(rename = "elapsedMs")]
    pub elapsed_ms: u64,
    #[serde(rename = "firestoreReads")]
    pub firestore_reads: u32,
}

pub async fn read(state: &AppState, barber_id: &str) -> Result<Option<CacheDoc>, String> {
    let parent = state
        .db
        .parent_path(PARENT_COLLECTION, barber_id)
        .map_err(|e| e.to_string())?;

    state
        .db
        .fluent()
        .select()
        .by_id_in(CACHE_COLLECTION)
        .parent(parent)
        .obj::<CacheDoc>()
        .one(CACHE_DOC_ID)
        .await
        .map_err(|e| e.to_string())
}

pub async fn write(state: &AppState, doc: &CacheDoc) -> Result<(), String> {
    let parent = state
        .db
        .parent_path(PARENT_COLLECTION, &doc.barber_id)
        .map_err(|e| e.to_string())?;

    state
        .db
        .fluent()
        .update()
        .in_col(CACHE_COLLECTION)
        .document_id(CACHE_DOC_ID)
        .parent(parent)
        .object(doc)
        .execute::<()>()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Safety cap: truncate each horizon to top `max_per_horizon` by matchScore desc
/// to stay under Firestore's 1 MiB doc limit on pathological shops.
pub fn cap_horizons(doc: &mut CacheDoc, max_per_horizon: usize) {
    for bucket in [
        &mut doc.horizons.h7,
        &mut doc.horizons.h14,
        &mut doc.horizons.h30,
    ] {
        if bucket.matched.len() > max_per_horizon {
            bucket
                .matched
                .sort_by(|a, b| b.match_score.cmp(&a.match_score));
            bucket.matched.truncate(max_per_horizon);
        }
        if bucket.due_without_gap.len() > max_per_horizon {
            bucket.due_without_gap.truncate(max_per_horizon);
        }
        if bucket.unmatched_gaps.len() > max_per_horizon {
            bucket.unmatched_gaps.truncate(max_per_horizon);
        }
    }
}

pub fn is_stale(computed_at: &str, ttl: chrono::Duration) -> bool {
    match DateTime::parse_from_rfc3339(computed_at) {
        Ok(dt) => Utc::now().signed_duration_since(dt.with_timezone(&Utc)) > ttl,
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::{CustomerCadence, Opportunity, ScheduleGap};
    use super::*;
    use chrono::{Duration, TimeZone, Utc};

    fn make_cadence(score_marker: i32) -> CustomerCadence {
        CustomerCadence {
            user_id: format!("u{}", score_marker),
            name: None,
            photo_url: None,
            phone_number: None,
            last_visit_date: Utc.timestamp_opt(0, 0).single().unwrap(),
            total_completed_visits: 1,
            median_interval_days: 30.0,
            due_date: Utc.timestamp_opt(0, 0).single().unwrap(),
            overdue_days: 0,
            is_overdue: false,
            recent_cancellation_count: 0,
            most_common_service: None,
            most_common_day_of_week: 0,
            most_common_time: None,
            preferred_specialist_id: None,
            typical_duration_minutes: 30,
        }
    }

    fn make_opp(score: i32) -> Opportunity {
        Opportunity {
            customer: make_cadence(score),
            gap: None,
            match_score: score,
            match_reason: None,
        }
    }

    fn make_gap(i: i32) -> ScheduleGap {
        ScheduleGap {
            date: Utc.timestamp_opt(i as i64, 0).single().unwrap(),
            start_time: "09:00".into(),
            end_time: "09:30".into(),
            duration_minutes: 30,
            specialist_id: "b1".into(),
        }
    }

    fn empty_cache_doc() -> CacheDoc {
        CacheDoc {
            schema_version: SCHEMA_VERSION,
            barber_id: "b1".into(),
            computed_at: "2026-01-01T00:00:00Z".into(),
            computed_from_input_at: "2026-01-01T00:00:00Z".into(),
            scheduler_run_id: "run1".into(),
            trigger: "scheduler".into(),
            horizons: HorizonBuckets::default(),
            stats: CacheStats::default(),
            last_error: None,
        }
    }

    #[test]
    fn cap_horizons_keeps_top_by_score_desc() {
        let mut doc = empty_cache_doc();
        doc.horizons.h7.matched = (0..200).map(make_opp).collect();
        cap_horizons(&mut doc, 100);
        assert_eq!(doc.horizons.h7.matched.len(), 100);
        assert_eq!(doc.horizons.h7.matched[0].match_score, 199);
        assert_eq!(doc.horizons.h7.matched[99].match_score, 100);
    }

    #[test]
    fn cap_horizons_truncates_due_without_gap_and_unmatched_gaps() {
        let mut doc = empty_cache_doc();
        doc.horizons.h14.due_without_gap = (0..150).map(make_opp).collect();
        doc.horizons.h14.unmatched_gaps = (0..150).map(make_gap).collect();
        cap_horizons(&mut doc, 50);
        assert_eq!(doc.horizons.h14.due_without_gap.len(), 50);
        assert_eq!(doc.horizons.h14.unmatched_gaps.len(), 50);
    }

    #[test]
    fn is_stale_true_for_old_timestamp() {
        let ts = (Utc::now() - Duration::minutes(10)).to_rfc3339();
        assert!(is_stale(&ts, Duration::minutes(5)));
    }

    #[test]
    fn is_stale_false_for_fresh_timestamp() {
        let ts = Utc::now().to_rfc3339();
        assert!(!is_stale(&ts, Duration::minutes(5)));
    }

    #[test]
    fn is_stale_true_for_unparseable() {
        assert!(is_stale("not-a-date", Duration::seconds(60)));
    }
}
