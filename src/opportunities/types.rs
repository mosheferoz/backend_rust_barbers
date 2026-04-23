use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------- Input types (what the repository supplies) ----------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BookingDoc {
    #[serde(rename = "userId", default)]
    pub user_id: String,
    #[serde(rename = "barberId", default)]
    pub barber_id: String,
    /// The scheduled date (midnight of the calendar day, Jerusalem-local interpretation).
    #[serde(default)]
    pub date: Option<DateTime<Utc>>,
    /// "HH:mm".
    #[serde(default)]
    pub time: String,
    /// 'completed' | 'cancelled' | 'noShow' | 'confirmed' | 'pending' | ...
    #[serde(default)]
    pub status: String,
    #[serde(rename = "specialistId", default)]
    pub specialist_id: String,
    #[serde(default)]
    pub services: Vec<String>,
    #[serde(rename = "totalDuration", default)]
    pub total_duration: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimeBlockDoc {
    #[serde(rename = "startTime", default)]
    pub start_time: Option<DateTime<Utc>>,
    #[serde(rename = "endTime", default)]
    pub end_time: Option<DateTime<Utc>>,
}

/// One entry in `openingHours` per weekday.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpeningHoursDay {
    #[serde(rename = "isOpen", default)]
    pub is_open: bool,
    /// "HH:mm" — also accepts legacy `openTime`.
    #[serde(default, alias = "openTime")]
    pub start: Option<String>,
    /// "HH:mm" — also accepts legacy `closeTime`.
    #[serde(default, alias = "closeTime")]
    pub end: Option<String>,
}

/// Keyed by lowercase weekday name: "sunday", "monday", …
pub type OpeningHours = HashMap<String, OpeningHoursDay>;

// ---------- Algorithm output types ----------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomerCadence {
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "photoUrl", skip_serializing_if = "Option::is_none")]
    pub photo_url: Option<String>,
    #[serde(rename = "phoneNumber", skip_serializing_if = "Option::is_none")]
    pub phone_number: Option<String>,
    #[serde(rename = "lastVisitDate")]
    pub last_visit_date: DateTime<Utc>,
    #[serde(rename = "totalCompletedVisits")]
    pub total_completed_visits: u32,
    #[serde(rename = "medianIntervalDays")]
    pub median_interval_days: f64,
    #[serde(rename = "dueDate")]
    pub due_date: DateTime<Utc>,
    #[serde(rename = "overdueDays")]
    pub overdue_days: i32,
    #[serde(rename = "isOverdue")]
    pub is_overdue: bool,
    #[serde(rename = "recentCancellationCount")]
    pub recent_cancellation_count: u32,
    #[serde(rename = "mostCommonService", skip_serializing_if = "Option::is_none")]
    pub most_common_service: Option<String>,
    /// 0 = Sunday, 6 = Saturday.
    #[serde(rename = "mostCommonDayOfWeek")]
    pub most_common_day_of_week: u32,
    #[serde(rename = "mostCommonTime", skip_serializing_if = "Option::is_none")]
    pub most_common_time: Option<String>,
    #[serde(rename = "preferredSpecialistId", skip_serializing_if = "Option::is_none")]
    pub preferred_specialist_id: Option<String>,
    #[serde(rename = "typicalDurationMinutes")]
    pub typical_duration_minutes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleGap {
    /// Midnight of the day the gap belongs to (UTC instant of local midnight).
    pub date: DateTime<Utc>,
    /// "HH:mm".
    #[serde(rename = "startTime")]
    pub start_time: String,
    #[serde(rename = "endTime")]
    pub end_time: String,
    #[serde(rename = "durationMinutes")]
    pub duration_minutes: u32,
    #[serde(rename = "specialistId")]
    pub specialist_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Opportunity {
    pub customer: CustomerCadence,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gap: Option<ScheduleGap>,
    #[serde(rename = "matchScore")]
    pub match_score: i32,
    #[serde(rename = "matchReason", skip_serializing_if = "Option::is_none")]
    pub match_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MatchOpportunitiesResult {
    pub matched: Vec<Opportunity>,
    #[serde(rename = "dueWithoutGap")]
    pub due_without_gap: Vec<Opportunity>,
    #[serde(rename = "unmatchedGaps")]
    pub unmatched_gaps: Vec<ScheduleGap>,
}
