use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use std::collections::{HashMap, HashSet};

use super::types::{
    BookingDoc, CustomerCadence, MatchOpportunitiesResult, OpeningHours, Opportunity, ScheduleGap,
    TimeBlockDoc,
};

// ---------- Hebrew match-reason strings (byte-equal to Dart) ----------

const REASON_SAME_DOW: &str = "אותו יום בשבוע";
const REASON_SAME_HOUR: &str = "שעה רגילה";
const REASON_SAME_SPECIALIST: &str = "ספר קבוע";
const REASON_JOIN: &str = " · ";

const DAY_NAMES: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

// ---------- compute_cadences ----------

#[derive(Debug, Clone)]
pub struct CadenceParams {
    pub barber_id: String,
    /// Multiplier on the median interval used to compute `dueDate`.
    pub due_factor: f64,
    /// Minimum visits needed to compute a cadence.
    pub min_visits_for_cadence: u32,
    /// Drop customers more than this many days overdue.
    pub max_overdue_days_to_show: i32,
    pub now: DateTime<Utc>,
    pub timezone: Tz,
}

impl CadenceParams {
    pub fn with_now(barber_id: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            barber_id: barber_id.into(),
            due_factor: 1.0,
            min_visits_for_cadence: 2,
            max_overdue_days_to_show: 90,
            now,
            timezone: chrono_tz::Asia::Jerusalem,
        }
    }
}

pub fn compute_cadences(
    bookings: &[BookingDoc],
    names: &HashMap<String, String>,
    photos: &HashMap<String, String>,
    phones: &HashMap<String, String>,
    params: &CadenceParams,
) -> Vec<CustomerCadence> {
    let tz = params.timezone;
    let today_date = local_date(params.now, tz);
    let today_utc = local_midnight_utc(today_date, tz);

    // Group bookings by userId. Drop empty userIds and bookings without a date.
    let mut grouped: HashMap<String, Vec<&BookingDoc>> = HashMap::new();
    let mut insertion_order: Vec<String> = Vec::new();
    for b in bookings {
        if b.user_id.is_empty() || b.date.is_none() {
            continue;
        }
        if !grouped.contains_key(&b.user_id) {
            insertion_order.push(b.user_id.clone());
        }
        grouped.entry(b.user_id.clone()).or_default().push(b);
    }

    // Customers with an active future booking are already on the calendar.
    let mut customers_with_future: HashSet<String> = HashSet::new();
    for (uid, list) in &grouped {
        for b in list {
            let d = b.date.expect("filtered out None dates above");
            if d >= today_utc && b.status != "cancelled" && b.status != "noShow" {
                customers_with_future.insert(uid.clone());
                break;
            }
        }
    }

    let mut cadences = Vec::new();

    for uid in &insertion_order {
        if customers_with_future.contains(uid) {
            continue;
        }
        let all_bookings = &grouped[uid];

        let mut completed: Vec<&&BookingDoc> = all_bookings
            .iter()
            .filter(|b| b.status == "completed")
            .collect();
        completed.sort_by(|a, b| a.date.unwrap().cmp(&b.date.unwrap()));

        if (completed.len() as u32) < params.min_visits_for_cadence {
            continue;
        }

        // Dates at local midnight (as UTC instants).
        let dates: Vec<DateTime<Utc>> = completed
            .iter()
            .map(|b| local_midnight_utc(local_date(b.date.unwrap(), tz), tz))
            .collect();

        let mut intervals: Vec<f64> = Vec::with_capacity(dates.len().saturating_sub(1));
        for i in 0..dates.len().saturating_sub(1) {
            let days = (dates[i + 1] - dates[i]).num_days();
            intervals.push(days as f64);
        }
        if intervals.is_empty() {
            continue;
        }

        let median_interval = median_f64(&intervals);
        let last_visit = *dates.last().unwrap();
        let due_add_days = (median_interval * params.due_factor).round() as i64;
        let due_date = last_visit + Duration::days(due_add_days);
        let overdue_days = (today_utc - due_date).num_days() as i32;

        if overdue_days > params.max_overdue_days_to_show {
            continue;
        }

        // Most common service (filter empty strings).
        let services_iter = completed
            .iter()
            .flat_map(|b| b.services.iter())
            .filter(|s| !s.is_empty())
            .cloned();
        let most_common_service = mode(services_iter);

        // Most common day-of-week in local tz: 0=Sun..6=Sat.
        let dows: Vec<u32> = completed
            .iter()
            .map(|b| {
                local_date(b.date.unwrap(), tz)
                    .weekday()
                    .num_days_from_sunday()
            })
            .collect();
        let most_common_dow = mode(dows.iter().copied()).unwrap_or(0);

        // Most common half-hour time bucket.
        let times_iter = completed.iter().filter_map(|b| bucket_half_hour(&b.time));
        let most_common_time = mode(times_iter);

        // Preferred specialist (filter empty strings).
        let specialists_iter = completed
            .iter()
            .map(|b| b.specialist_id.clone())
            .filter(|s| !s.is_empty());
        let preferred_specialist = mode(specialists_iter);

        // Typical duration.
        let durations: Vec<f64> = completed
            .iter()
            .map(|b| b.total_duration as f64)
            .filter(|d| *d > 0.0)
            .collect();
        let typical_duration_minutes: u32 = if durations.is_empty() {
            30
        } else {
            median_f64(&durations).round() as u32
        };

        // Recent cancellations: last 3 bookings by date desc, count cancelled/noShow.
        let mut sorted_desc: Vec<&&BookingDoc> = all_bookings.iter().collect();
        sorted_desc.sort_by(|a, b| b.date.unwrap().cmp(&a.date.unwrap()));
        let recent_cancellation_count = sorted_desc
            .iter()
            .take(3)
            .filter(|b| b.status == "cancelled" || b.status == "noShow")
            .count() as u32;

        cadences.push(CustomerCadence {
            user_id: uid.clone(),
            name: names.get(uid).cloned(),
            photo_url: photos.get(uid).cloned(),
            phone_number: phones.get(uid).cloned(),
            last_visit_date: last_visit,
            total_completed_visits: completed.len() as u32,
            median_interval_days: median_interval,
            due_date,
            overdue_days,
            is_overdue: overdue_days >= 0,
            recent_cancellation_count,
            most_common_service,
            most_common_day_of_week: most_common_dow,
            most_common_time,
            preferred_specialist_id: preferred_specialist,
            typical_duration_minutes,
        });
    }

    cadences
}

// ---------- find_gaps ----------

#[derive(Debug, Clone)]
pub struct FindGapsParams {
    pub barber_id: String,
    pub horizon_days: u32,
    pub slot_minutes: u32,
    pub now: DateTime<Utc>,
    pub timezone: Tz,
}

impl FindGapsParams {
    pub fn with_now(barber_id: impl Into<String>, horizon_days: u32, now: DateTime<Utc>) -> Self {
        Self {
            barber_id: barber_id.into(),
            horizon_days,
            slot_minutes: 30,
            now,
            timezone: chrono_tz::Asia::Jerusalem,
        }
    }
}

pub fn find_gaps(
    opening_hours: &OpeningHours,
    bookings: &[BookingDoc],
    time_blocks: &[TimeBlockDoc],
    params: &FindGapsParams,
) -> Vec<ScheduleGap> {
    let tz = params.timezone;
    let today_date = local_date(params.now, tz);
    let today_utc = local_midnight_utc(today_date, tz);
    let end_date = today_date + Duration::days(params.horizon_days as i64);
    let end_utc = local_midnight_utc(end_date, tz);
    let slot_minutes = params.slot_minutes as i32;

    // Bucket busy ranges by local day → list of (start_min, end_min) offsets from local midnight.
    let mut busy_by_day: HashMap<NaiveDate, Vec<(i32, i32)>> = HashMap::new();

    for b in bookings {
        let Some(date) = b.date else {
            continue;
        };
        if b.status == "cancelled" || b.status == "noShow" {
            continue;
        }
        let Some(start_mins) = parse_hm(&b.time) else {
            continue;
        };
        let booking_date = local_date(date, tz);
        let start_utc = local_midnight_utc(booking_date, tz) + Duration::minutes(start_mins as i64);
        if start_utc < today_utc || start_utc >= end_utc {
            continue;
        }
        let end_utc_booking = start_utc + Duration::minutes(b.total_duration as i64);
        add_range_utc(&mut busy_by_day, start_utc, end_utc_booking, tz);
    }

    for blk in time_blocks {
        let (Some(s), Some(e)) = (blk.start_time, blk.end_time) else {
            continue;
        };
        if e < today_utc || s >= end_utc {
            continue;
        }
        add_range_utc(&mut busy_by_day, s, e, tz);
    }

    let mut gaps = Vec::new();
    for i in 0..params.horizon_days as i64 {
        let day = today_date + Duration::days(i);
        let dow = day.weekday().num_days_from_sunday();
        let dow_name = DAY_NAMES[dow as usize];
        let Some(day_config) = opening_hours.get(dow_name) else {
            continue;
        };
        if !day_config.is_open {
            continue;
        }
        let (Some(open_s), Some(close_s)) = (day_config.start.as_deref(), day_config.end.as_deref())
        else {
            continue;
        };
        let Some(open_min) = parse_hm(open_s) else {
            continue;
        };
        let Some(close_min) = parse_hm(close_s) else {
            continue;
        };
        if close_min - open_min < slot_minutes {
            continue;
        }

        let empty = Vec::new();
        let day_busy = busy_by_day.get(&day).unwrap_or(&empty);

        let day_midnight_utc = local_midnight_utc(day, tz);

        let mut slot_start = open_min;
        while slot_start + slot_minutes <= close_min {
            let slot_end = slot_start + slot_minutes;
            let overlaps = day_busy
                .iter()
                .any(|(rs, re)| *rs < slot_end && *re > slot_start);
            if !overlaps {
                gaps.push(ScheduleGap {
                    date: day_midnight_utc,
                    start_time: format_hm(slot_start),
                    end_time: format_hm(slot_end),
                    duration_minutes: slot_minutes as u32,
                    specialist_id: params.barber_id.clone(),
                });
            }
            slot_start += slot_minutes;
        }
    }

    gaps
}

fn add_range_utc(
    busy: &mut HashMap<NaiveDate, Vec<(i32, i32)>>,
    s_utc: DateTime<Utc>,
    e_utc: DateTime<Utc>,
    tz: Tz,
) {
    // A range may straddle midnight in local time; split per local day.
    let mut cursor = s_utc;
    while cursor < e_utc {
        let day = local_date(cursor, tz);
        let day_midnight = local_midnight_utc(day, tz);
        let Some(next_day) = day.succ_opt() else {
            break;
        };
        let next_midnight = local_midnight_utc(next_day, tz);
        let segment_end = if e_utc < next_midnight {
            e_utc
        } else {
            next_midnight
        };
        let start_min = (cursor - day_midnight).num_minutes() as i32;
        let end_min = (segment_end - day_midnight).num_minutes() as i32;
        busy.entry(day).or_default().push((start_min, end_min));
        cursor = segment_end;
    }
}

// ---------- match_opportunities ----------

pub fn match_opportunities(
    cadences: Vec<CustomerCadence>,
    gaps: Vec<ScheduleGap>,
) -> MatchOpportunitiesResult {
    let tz = chrono_tz::Asia::Jerusalem;

    let mut sorted_cadences = cadences;
    // Sort by overdueDays descending; stable sort preserves original order for ties.
    sorted_cadences.sort_by(|a, b| b.overdue_days.cmp(&a.overdue_days));

    let mut claimed = vec![false; gaps.len()];
    let mut matched: Vec<Opportunity> = Vec::new();
    let mut due_without_gap: Vec<Opportunity> = Vec::new();

    for cadence in sorted_cadences {
        let mut best_idx: i32 = -1;
        let mut best_score: i32 = i32::MIN;
        let mut best_reason: Option<String> = None;
        let mut best_fits_duration = false;

        for (i, gap) in gaps.iter().enumerate() {
            if claimed[i] {
                continue;
            }
            let (score, reason) = score_opportunity(&cadence, gap, tz);
            let fits = gap.duration_minutes >= cadence.typical_duration_minutes;

            let should_replace = best_idx == -1
                || (fits && !best_fits_duration)
                || (fits == best_fits_duration && score > best_score);

            if should_replace {
                best_idx = i as i32;
                best_score = score;
                best_reason = reason;
                best_fits_duration = fits;
            }
        }

        if best_idx == -1 || best_score <= 0 {
            due_without_gap.push(Opportunity {
                customer: cadence,
                gap: None,
                match_score: 0,
                match_reason: None,
            });
            continue;
        }

        claimed[best_idx as usize] = true;
        matched.push(Opportunity {
            customer: cadence,
            gap: Some(gaps[best_idx as usize].clone()),
            match_score: best_score,
            match_reason: best_reason,
        });
    }

    let unmatched_gaps: Vec<ScheduleGap> = gaps
        .into_iter()
        .enumerate()
        .filter_map(|(i, g)| if claimed[i] { None } else { Some(g) })
        .collect();

    MatchOpportunitiesResult {
        matched,
        due_without_gap,
        unmatched_gaps,
    }
}

fn score_opportunity(c: &CustomerCadence, g: &ScheduleGap, tz: Tz) -> (i32, Option<String>) {
    let mut s: i32 = 0;
    let mut r: Vec<&str> = Vec::new();

    let gap_dow = local_date(g.date, tz).weekday().num_days_from_sunday();
    if gap_dow == c.most_common_day_of_week {
        s += 30;
        r.push(REASON_SAME_DOW);
    }

    if let Some(ct) = &c.most_common_time {
        let gh: i32 = g
            .start_time
            .split(':')
            .next()
            .unwrap_or("")
            .parse()
            .unwrap_or(0);
        let th: i32 = ct.split(':').next().unwrap_or("").parse().unwrap_or(0);
        let d = (gh - th).abs();
        if d == 0 {
            s += 25;
        } else if d == 1 {
            s += 18;
        } else if d == 2 {
            s += 10;
        }
        if d <= 2 {
            r.push(REASON_SAME_HOUR);
        }
    }

    if let Some(ps) = &c.preferred_specialist_id {
        if ps == &g.specialist_id {
            s += 20;
            r.push(REASON_SAME_SPECIALIST);
        }
    }

    if g.duration_minutes >= c.typical_duration_minutes {
        s += 15;
    } else {
        s -= 10;
    }

    s += ((c.overdue_days as f64) / 7.0).clamp(0.0, 4.0).floor() as i32 * 2;
    s -= (c.recent_cancellation_count as i32) * 8;
    if c.total_completed_visits < 4 {
        s -= 5;
    }

    let clamped = s.clamp(0, 100);
    let reason = if r.is_empty() {
        None
    } else {
        Some(r.join(REASON_JOIN))
    };
    (clamped, reason)
}

// ---------- Helpers ----------

fn local_date(dt: DateTime<Utc>, tz: Tz) -> NaiveDate {
    dt.with_timezone(&tz).date_naive()
}

fn local_midnight_utc(date: NaiveDate, tz: Tz) -> DateTime<Utc> {
    let lr = tz.with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0);
    lr.single()
        .or_else(|| lr.earliest())
        .expect("midnight should exist in Jerusalem tz")
        .with_timezone(&Utc)
}

fn median_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let mid = n / 2;
    if n % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    }
}

/// Mode that preserves insertion order on ties (like Dart's LinkedHashMap).
/// Returns `None` for empty input.
fn mode<T, I>(iter: I) -> Option<T>
where
    T: Eq + Clone,
    I: IntoIterator<Item = T>,
{
    let mut order: Vec<T> = Vec::new();
    let mut counts: Vec<i32> = Vec::new();
    for item in iter {
        match order.iter().position(|x| x == &item) {
            Some(idx) => counts[idx] += 1,
            None => {
                order.push(item);
                counts.push(1);
            }
        }
    }
    if order.is_empty() {
        return None;
    }
    let mut best_idx = 0usize;
    for i in 1..counts.len() {
        if counts[i] > counts[best_idx] {
            best_idx = i;
        }
    }
    Some(order[best_idx].clone())
}

fn bucket_half_hour(time: &str) -> Option<String> {
    if time.is_empty() {
        return None;
    }
    let mut parts = time.splitn(3, ':');
    let h_str = parts.next()?;
    let m_str = parts.next()?;
    let h: u32 = h_str.parse().ok()?;
    let m: u32 = m_str.parse().ok()?;
    let bucket_min = if m < 30 { 0 } else { 30 };
    Some(format!("{:02}:{:02}", h, bucket_min))
}

fn parse_hm(hm: &str) -> Option<i32> {
    if hm.is_empty() {
        return None;
    }
    let mut parts = hm.splitn(3, ':');
    let h_str = parts.next()?;
    let m_str = parts.next()?;
    let h: i32 = h_str.parse().ok()?;
    let m: i32 = m_str.parse().ok()?;
    Some(h * 60 + m)
}

fn format_hm(total_minutes: i32) -> String {
    let h = total_minutes / 60;
    let m = total_minutes % 60;
    format!("{:02}:{:02}", h, m)
}

// ==============================================================================
// Tests — mirror `test/features/opportunities/**` from Dart, one-for-one.
// ==============================================================================

#[cfg(test)]
mod tests {
    use super::super::types::OpeningHoursDay;
    use super::*;
    use std::collections::HashMap;

    // ---------- fixture helpers ----------

    const JERUSALEM: Tz = chrono_tz::Asia::Jerusalem;

    /// UTC instant for local Jerusalem midnight on a calendar day.
    fn jer_midnight(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        JERUSALEM
            .with_ymd_and_hms(y, m, d, 0, 0, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc)
    }

    /// UTC instant for a specific local Jerusalem wall-clock time.
    fn jer_dt(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        JERUSALEM
            .with_ymd_and_hms(y, m, d, h, mi, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc)
    }

    #[allow(clippy::too_many_arguments)]
    fn booking(
        user_id: &str,
        date: DateTime<Utc>,
        time: &str,
        status: &str,
        specialist: &str,
        services: &[&str],
        total_duration: i32,
    ) -> BookingDoc {
        BookingDoc {
            user_id: user_id.into(),
            barber_id: "barber1".into(),
            date: Some(date),
            time: time.into(),
            status: status.into(),
            specialist_id: specialist.into(),
            services: services.iter().map(|s| s.to_string()).collect(),
            total_duration,
        }
    }

    /// Mirrors Dart `_b(userId:, date:, time:..., status:...)` defaults.
    fn b(user_id: &str, date: DateTime<Utc>) -> BookingDoc {
        booking(user_id, date, "10:00", "completed", "barber1", &["cut"], 30)
    }

    fn b_time(user_id: &str, date: DateTime<Utc>, time: &str) -> BookingDoc {
        booking(user_id, date, time, "completed", "barber1", &["cut"], 30)
    }

    fn b_status(user_id: &str, date: DateTime<Utc>, status: &str) -> BookingDoc {
        booking(user_id, date, "10:00", status, "barber1", &["cut"], 30)
    }

    fn open(start: &str, end: &str) -> OpeningHoursDay {
        OpeningHoursDay {
            is_open: true,
            start: Some(start.into()),
            end: Some(end.into()),
        }
    }

    fn closed() -> OpeningHoursDay {
        OpeningHoursDay {
            is_open: false,
            start: None,
            end: None,
        }
    }

    fn cadence_params(now: DateTime<Utc>, max_overdue: i32, min_visits: u32) -> CadenceParams {
        CadenceParams {
            barber_id: "barber1".into(),
            due_factor: 1.0,
            min_visits_for_cadence: min_visits,
            max_overdue_days_to_show: max_overdue,
            now,
            timezone: JERUSALEM,
        }
    }

    fn find_gaps_params(now: DateTime<Utc>, horizon: u32) -> FindGapsParams {
        FindGapsParams {
            barber_id: "b1".into(),
            horizon_days: horizon,
            slot_minutes: 30,
            now,
            timezone: JERUSALEM,
        }
    }

    // =========================================================================
    // compute_cadences tests (mirror compute_customer_cadences_usecase_test.dart)
    // =========================================================================

    #[test]
    fn median_of_30_28_35_29_day_intervals_is_29_5() {
        // Build dates so consecutive deltas are 30, 28, 35, 29 (4 intervals).
        let d0 = jer_midnight(2025, 1, 1);
        let d1 = d0 + Duration::days(30);
        let d2 = d1 + Duration::days(28);
        let d3 = d2 + Duration::days(35);
        let d4 = d3 + Duration::days(29);

        let bookings = vec![
            b("u1", d0),
            b("u1", d1),
            b("u1", d2),
            b("u1", d3),
            b("u1", d4),
        ];

        let today = jer_midnight(2026, 4, 16);
        let result = compute_cadences(
            &bookings,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 100_000, 2),
        );

        assert_eq!(result.len(), 1);
        assert!((result[0].median_interval_days - 29.5).abs() < 0.001);
        assert_eq!(result[0].total_completed_visits, 5);
    }

    #[test]
    fn single_completed_visit_not_returned() {
        let today = jer_midnight(2026, 4, 16);
        let result = compute_cadences(
            &[b("u1", jer_midnight(2026, 1, 1))],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 100_000, 2),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn all_cancelled_history_no_cadence_emitted() {
        let today = jer_midnight(2026, 4, 16);
        let bookings = vec![
            b_status("u1", jer_midnight(2026, 1, 1), "cancelled"),
            b_status("u1", jer_midnight(2026, 2, 1), "noShow"),
            b_status("u1", jer_midnight(2026, 3, 1), "cancelled"),
        ];
        let result = compute_cadences(
            &bookings,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 100_000, 2),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn mixed_history_recent_cancellation_count_last_three_desc() {
        let today = jer_midnight(2026, 4, 16);
        let bookings = vec![
            b("u1", jer_midnight(2025, 9, 1)),
            b("u1", jer_midnight(2025, 10, 1)),
            b_status("u1", jer_midnight(2026, 1, 1), "cancelled"),
            b_status("u1", jer_midnight(2026, 2, 1), "noShow"),
        ];
        let result = compute_cadences(
            &bookings,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 100_000, 2),
        );
        assert_eq!(result.len(), 1);
        // Last 3 by date desc: 2026-02-01 (noShow), 2026-01-01 (cancelled),
        // 2025-10-01 (completed) → 2 negatives.
        assert_eq!(result[0].recent_cancellation_count, 2);
    }

    #[test]
    fn customer_with_active_future_booking_is_excluded() {
        let today = jer_midnight(2026, 4, 16);
        let bookings = vec![
            b("u1", jer_midnight(2026, 1, 1)),
            b("u1", jer_midnight(2026, 2, 1)),
            b_status("u1", jer_midnight(2026, 4, 21), "confirmed"),
        ];
        let result = compute_cadences(
            &bookings,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 100_000, 2),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn overdue_beyond_max_overdue_days_is_filtered() {
        let today = jer_midnight(2026, 4, 16);
        // Two visits 30 days apart → median 30. Last visit > 100 days ago.
        let last_visit = today - Duration::days(200);
        let prior_visit = last_visit - Duration::days(30);
        let result = compute_cadences(
            &[b("u1", prior_visit), b("u1", last_visit)],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 90, 2),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn most_common_day_of_week_uses_zero_sunday_to_six_saturday() {
        // 2025-10-05 is Sunday. Three Sundays.
        let s0 = jer_midnight(2025, 10, 5);
        let s1 = jer_midnight(2025, 10, 12);
        let s2 = jer_midnight(2025, 10, 19);
        let today = jer_midnight(2026, 4, 16);
        let result = compute_cadences(
            &[b("u1", s0), b("u1", s1), b("u1", s2)],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 100_000, 2),
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].most_common_day_of_week, 0);
    }

    #[test]
    fn user_metadata_passed_through_when_provided_in_maps() {
        let today = jer_midnight(2026, 4, 16);
        let mut names = HashMap::new();
        names.insert("u1".to_string(), "Moshe".to_string());
        let mut photos = HashMap::new();
        photos.insert("u1".to_string(), "https://x/y.png".to_string());
        let mut phones = HashMap::new();
        phones.insert("u1".to_string(), "+972500000000".to_string());

        let result = compute_cadences(
            &[
                b("u1", jer_midnight(2026, 1, 1)),
                b("u1", jer_midnight(2026, 2, 1)),
            ],
            &names,
            &photos,
            &phones,
            &cadence_params(today, 100_000, 2),
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name.as_deref(), Some("Moshe"));
        assert_eq!(result[0].photo_url.as_deref(), Some("https://x/y.png"));
        assert_eq!(result[0].phone_number.as_deref(), Some("+972500000000"));
    }

    // =========================================================================
    // find_gaps tests (mirror find_schedule_gaps_usecase_test.dart)
    // =========================================================================

    fn oh(days: &[(&str, OpeningHoursDay)]) -> OpeningHours {
        days.iter().map(|(n, d)| (n.to_string(), d.clone())).collect()
    }

    fn gap_booking(date: DateTime<Utc>, time: &str, status: &str, total: i32) -> BookingDoc {
        booking("u1", date, time, status, "b1", &["cut"], total)
    }

    fn time_block(start: DateTime<Utc>, end: DateTime<Utc>) -> TimeBlockDoc {
        TimeBlockDoc {
            start_time: Some(start),
            end_time: Some(end),
        }
    }

    #[test]
    fn day_marked_is_open_false_yields_zero_gaps() {
        // 2026-04-19 is a Sunday.
        let today = jer_midnight(2026, 4, 19);
        let gaps = find_gaps(
            &oh(&[("sunday", closed())]),
            &[],
            &[],
            &find_gaps_params(today, 1),
        );
        assert!(gaps.is_empty());
    }

    #[test]
    fn sixty_min_booking_leaves_four_thirty_min_slots() {
        let today = jer_midnight(2026, 4, 19);
        let gaps = find_gaps(
            &oh(&[("sunday", open("09:00", "12:00"))]),
            &[gap_booking(today, "09:00", "confirmed", 60)],
            &[],
            &find_gaps_params(today, 1),
        );
        let starts: Vec<&str> = gaps.iter().map(|g| g.start_time.as_str()).collect();
        assert_eq!(starts, vec!["10:00", "10:30", "11:00", "11:30"]);
        for g in &gaps {
            assert_eq!(g.duration_minutes, 30);
            assert_eq!(g.specialist_id, "b1");
            assert_eq!(g.date, today);
        }
    }

    #[test]
    fn time_block_11_to_12_destroys_11_00_and_11_30_slots() {
        let today = jer_midnight(2026, 4, 19);
        let gaps = find_gaps(
            &oh(&[("sunday", open("09:00", "12:00"))]),
            &[],
            &[time_block(jer_dt(2026, 4, 19, 11, 0), jer_dt(2026, 4, 19, 12, 0))],
            &find_gaps_params(today, 1),
        );
        let starts: Vec<&str> = gaps.iter().map(|g| g.start_time.as_str()).collect();
        assert_eq!(starts, vec!["09:00", "09:30", "10:00", "10:30"]);
    }

    #[test]
    fn cancelled_bookings_do_not_block_slots() {
        let today = jer_midnight(2026, 4, 19);
        let gaps = find_gaps(
            &oh(&[("sunday", open("09:00", "10:00"))]),
            &[gap_booking(today, "09:00", "cancelled", 60)],
            &[],
            &find_gaps_params(today, 1),
        );
        let starts: Vec<&str> = gaps.iter().map(|g| g.start_time.as_str()).collect();
        assert_eq!(starts, vec!["09:00", "09:30"]);
    }

    #[test]
    fn empty_open_block_yields_no_gaps() {
        let today = jer_midnight(2026, 4, 19);
        let gaps = find_gaps(
            &oh(&[("sunday", open("09:00", "09:00"))]),
            &[],
            &[],
            &find_gaps_params(today, 1),
        );
        assert!(gaps.is_empty());
    }

    // =========================================================================
    // match_opportunities tests (mirror match_opportunities_usecase_test.dart)
    // =========================================================================

    #[allow(clippy::too_many_arguments)]
    fn cadence(
        user_id: &str,
        overdue_days: i32,
        mc_dow: u32,
        mc_time: Option<&str>,
        preferred_specialist: Option<&str>,
        typical_duration: u32,
        total_completed_visits: u32,
        recent_cancellation_count: u32,
    ) -> CustomerCadence {
        let now = jer_midnight(2026, 4, 19);
        let due_date = now - Duration::days(overdue_days as i64);
        CustomerCadence {
            user_id: user_id.into(),
            name: Some(user_id.into()),
            photo_url: None,
            phone_number: None,
            last_visit_date: due_date,
            total_completed_visits,
            median_interval_days: 30.0,
            due_date,
            overdue_days,
            is_overdue: overdue_days >= 0,
            recent_cancellation_count,
            most_common_service: Some("cut".into()),
            most_common_day_of_week: mc_dow,
            most_common_time: mc_time.map(|s| s.into()),
            preferred_specialist_id: preferred_specialist.map(|s| s.into()),
            typical_duration_minutes: typical_duration,
        }
    }

    fn gap(
        date: DateTime<Utc>,
        start_time: &str,
        end_time: &str,
        duration: u32,
        specialist: &str,
    ) -> ScheduleGap {
        ScheduleGap {
            date,
            start_time: start_time.into(),
            end_time: end_time.into(),
            duration_minutes: duration,
            specialist_id: specialist.into(),
        }
    }

    fn default_gap(date: DateTime<Utc>, start_time: &str) -> ScheduleGap {
        gap(date, start_time, "10:30", 30, "b1")
    }

    #[test]
    fn empty_inputs_return_empty_results() {
        let res = match_opportunities(vec![], vec![]);
        assert!(res.matched.is_empty());
        assert!(res.due_without_gap.is_empty());
        assert!(res.unmatched_gaps.is_empty());
    }

    #[test]
    fn no_gaps_cadences_land_in_due_without_gap() {
        let c = cadence("u1", 5, 0, None, None, 30, 5, 0);
        let res = match_opportunities(vec![c], vec![]);
        assert!(res.matched.is_empty());
        assert_eq!(res.due_without_gap.len(), 1);
        assert_eq!(res.due_without_gap[0].customer.user_id, "u1");
        assert!(res.due_without_gap[0].gap.is_none());
    }

    #[test]
    fn two_cadences_three_gaps_both_matched_by_dow() {
        let sun = jer_midnight(2026, 4, 19);
        let mon = jer_midnight(2026, 4, 20);

        let c1 = cadence("sundayLover", 10, 0, Some("09:00"), None, 30, 5, 0);
        let c2 = cadence("mondayLover", 5, 1, Some("14:00"), None, 30, 5, 0);

        let g_sun_9 = default_gap(sun, "09:00");
        let g_mon_14 = default_gap(mon, "14:00");
        let g_sun_14 = default_gap(sun, "14:00");

        let res = match_opportunities(
            vec![c1, c2],
            vec![g_sun_9.clone(), g_mon_14.clone(), g_sun_14.clone()],
        );

        assert_eq!(res.matched.len(), 2);
        let by_user: HashMap<String, &Opportunity> = res
            .matched
            .iter()
            .map(|o| (o.customer.user_id.clone(), o))
            .collect();
        assert_eq!(by_user["sundayLover"].gap.as_ref(), Some(&g_sun_9));
        assert_eq!(by_user["mondayLover"].gap.as_ref(), Some(&g_mon_14));
        assert_eq!(res.unmatched_gaps, vec![g_sun_14]);
    }

    #[test]
    fn short_gap_skipped_when_longer_gap_fits() {
        let sun = jer_midnight(2026, 4, 19);
        let c = cadence("u1", 5, 0, None, None, 60, 5, 0);
        let fits = gap(sun, "09:00", "10:00", 60, "b1");
        let too_short = gap(sun, "11:00", "11:30", 30, "b1");

        let res = match_opportunities(vec![c], vec![too_short.clone(), fits.clone()]);

        assert_eq!(res.matched.len(), 1);
        assert_eq!(res.matched[0].gap.as_ref(), Some(&fits));
        assert_eq!(res.unmatched_gaps, vec![too_short]);
    }

    #[test]
    fn preferred_specialist_match_adds_score_and_reason() {
        let sun = jer_midnight(2026, 4, 19);
        let c = cadence("u1", 5, 0, None, Some("specA"), 30, 5, 0);
        let wrong = gap(sun, "09:00", "09:30", 30, "specB");
        let right = gap(sun, "10:00", "10:30", 30, "specA");
        let res = match_opportunities(vec![c], vec![wrong, right.clone()]);
        assert_eq!(res.matched.len(), 1);
        assert_eq!(res.matched[0].gap.as_ref(), Some(&right));
        assert!(res.matched[0]
            .match_reason
            .as_ref()
            .unwrap()
            .contains("ספר קבוע"));
    }

    #[test]
    fn greedy_more_overdue_cadence_picks_first() {
        let sun = jer_midnight(2026, 4, 19);
        let c1 = cadence("mild", 1, 0, Some("09:00"), None, 30, 5, 0);
        let c2 = cadence("urgent", 30, 0, Some("09:00"), None, 30, 5, 0);
        let g0 = default_gap(sun, "09:00");
        let g1 = default_gap(sun, "10:00");

        let res = match_opportunities(vec![c1, c2], vec![g0.clone(), g1.clone()]);
        let by_user: HashMap<String, &Opportunity> = res
            .matched
            .iter()
            .map(|o| (o.customer.user_id.clone(), o))
            .collect();
        assert_eq!(by_user["urgent"].gap.as_ref(), Some(&g0));
        assert_eq!(by_user["mild"].gap.as_ref(), Some(&g1));
    }

    // =========================================================================
    // scenario_end_to_end tests (mirror scenario_end_to_end_test.dart)
    // =========================================================================

    fn scenario_opening_hours() -> OpeningHours {
        oh(&[
            ("sunday", open("09:00", "18:00")),
            ("monday", open("09:00", "18:00")),
            ("tuesday", open("09:00", "18:00")),
            ("wednesday", open("09:00", "18:00")),
            ("thursday", open("09:00", "18:00")),
            ("friday", closed()),
            ("saturday", closed()),
        ])
    }

    fn alice_bookings() -> Vec<BookingDoc> {
        vec![
            b_time("u1", jer_midnight(2025, 12, 21), "10:00"),
            b_time("u1", jer_midnight(2026, 1, 18), "10:00"),
            b_time("u1", jer_midnight(2026, 2, 15), "10:00"),
            b_time("u1", jer_midnight(2026, 3, 15), "10:00"),
        ]
    }

    fn clara_bookings() -> Vec<BookingDoc> {
        vec![
            b_time("u3", jer_midnight(2026, 2, 17), "14:00"),
            b_time("u3", jer_midnight(2026, 3, 17), "14:00"),
            b_time("u3", jer_midnight(2026, 4, 14), "14:00"),
        ]
    }

    fn bob_bookings() -> Vec<BookingDoc> {
        vec![b("u2", jer_midnight(2026, 3, 30))]
    }

    fn blocker_bookings() -> Vec<BookingDoc> {
        vec![booking(
            "walkin1",
            jer_midnight(2026, 4, 20),
            "10:00",
            "confirmed",
            "barber1",
            &["haircut"],
            60,
        )]
    }

    fn scenario_all_bookings() -> Vec<BookingDoc> {
        let mut v = alice_bookings();
        v.extend(clara_bookings());
        v.extend(bob_bookings());
        v.extend(blocker_bookings());
        v
    }

    #[test]
    fn scenario_alice_sunday_regular_detected_as_overdue() {
        let today = jer_midnight(2026, 4, 19);
        let mut names = HashMap::new();
        names.insert("u1".into(), "Alice".into());
        names.insert("u3".into(), "Clara".into());
        let mut phones = HashMap::new();
        phones.insert("u1".into(), "+972501234567".into());
        phones.insert("u3".into(), "+972502222222".into());

        let cadences = compute_cadences(
            &scenario_all_bookings(),
            &names,
            &HashMap::new(),
            &phones,
            &cadence_params(today, 90, 2),
        );

        assert_eq!(cadences.len(), 2, "Bob excluded; Alice + Clara remain");
        let by_id: HashMap<String, &CustomerCadence> =
            cadences.iter().map(|c| (c.user_id.clone(), c)).collect();
        assert!(by_id.contains_key("u1"));
        assert!(by_id.contains_key("u3"));
        assert!(!by_id.contains_key("u2"), "Bob has only one visit");

        let alice = by_id["u1"];
        assert_eq!(alice.total_completed_visits, 4);
        assert!((alice.median_interval_days - 28.0).abs() < 0.001);
        assert!(
            (5..=9).contains(&alice.overdue_days),
            "overdue_days = {}, expected 5..=9",
            alice.overdue_days
        );
        assert!(alice.is_overdue);
        assert_eq!(alice.most_common_day_of_week, 0);
        assert_eq!(alice.most_common_time.as_deref(), Some("10:00"));
        assert_eq!(alice.preferred_specialist_id.as_deref(), Some("barber1"));
        assert_eq!(alice.phone_number.as_deref(), Some("+972501234567"));
        assert_eq!(alice.typical_duration_minutes, 30);

        let clara = by_id["u3"];
        assert!(!clara.is_overdue);
        assert_eq!(clara.most_common_day_of_week, 2);
        assert_eq!(clara.most_common_time.as_deref(), Some("14:00"));
    }

    #[test]
    fn scenario_find_gaps_today_sunday_yields_18_open_slots() {
        let today = jer_midnight(2026, 4, 19);
        let gaps = find_gaps(
            &scenario_opening_hours(),
            &scenario_all_bookings(),
            &[],
            &FindGapsParams {
                barber_id: "barber1".into(),
                horizon_days: 1,
                slot_minutes: 30,
                now: today,
                timezone: JERUSALEM,
            },
        );
        assert_eq!(gaps.len(), 18);
        assert_eq!(gaps.first().unwrap().start_time, "09:00");
        assert_eq!(gaps.last().unwrap().start_time, "17:30");
        assert!(gaps.iter().all(|g| g.date == today));
        assert!(gaps.iter().any(|g| g.start_time == "10:00"));
    }

    #[test]
    fn scenario_monday_blocker_carves_hole() {
        let today = jer_midnight(2026, 4, 19);
        let monday = jer_midnight(2026, 4, 20);
        let gaps = find_gaps(
            &scenario_opening_hours(),
            &scenario_all_bookings(),
            &[],
            &FindGapsParams {
                barber_id: "barber1".into(),
                horizon_days: 2,
                slot_minutes: 30,
                now: today,
                timezone: JERUSALEM,
            },
        );
        let monday_gaps: Vec<&ScheduleGap> = gaps.iter().filter(|g| g.date == monday).collect();
        assert_eq!(monday_gaps.len(), 16);
        let starts: Vec<&str> = monday_gaps.iter().map(|g| g.start_time.as_str()).collect();
        assert!(!starts.contains(&"10:00"));
        assert!(!starts.contains(&"10:30"));
        assert!(starts.contains(&"09:30"));
        assert!(starts.contains(&"11:00"));
    }

    #[test]
    fn scenario_full_pipeline_alice_today_strong_score_clara_tuesday() {
        let today = jer_midnight(2026, 4, 19);
        let mut names = HashMap::new();
        names.insert("u1".into(), "Alice".into());
        names.insert("u3".into(), "Clara".into());
        let mut phones = HashMap::new();
        phones.insert("u1".into(), "+972501234567".into());
        phones.insert("u3".into(), "+972502222222".into());

        let cadences = compute_cadences(
            &scenario_all_bookings(),
            &names,
            &HashMap::new(),
            &phones,
            &cadence_params(today, 90, 2),
        );

        let gaps = find_gaps(
            &scenario_opening_hours(),
            &scenario_all_bookings(),
            &[],
            &FindGapsParams {
                barber_id: "barber1".into(),
                horizon_days: 14,
                slot_minutes: 30,
                now: today,
                timezone: JERUSALEM,
            },
        );

        let result = match_opportunities(cadences, gaps);

        assert_eq!(result.matched.len(), 2);
        assert!(result.due_without_gap.is_empty());

        let alice_opp = result
            .matched
            .iter()
            .find(|o| o.customer.user_id == "u1")
            .unwrap();
        assert!(
            alice_opp.match_score >= 80,
            "Alice match_score = {}, expected >= 80",
            alice_opp.match_score
        );
        let alice_gap = alice_opp.gap.as_ref().unwrap();
        assert_eq!(alice_gap.date, today);
        assert_eq!(alice_gap.start_time, "10:00");
        let alice_reason = alice_opp.match_reason.as_ref().unwrap();
        assert!(alice_reason.contains("אותו יום בשבוע"));
        assert!(alice_reason.contains("שעה רגילה"));
        assert!(alice_reason.contains("ספר קבוע"));

        let clara_opp = result
            .matched
            .iter()
            .find(|o| o.customer.user_id == "u3")
            .unwrap();
        let clara_gap = clara_opp.gap.as_ref().unwrap();
        assert_eq!(
            local_date(clara_gap.date, JERUSALEM)
                .weekday()
                .num_days_from_sunday(),
            2,
            "Clara's gap should be on Tuesday"
        );
        assert_eq!(clara_gap.start_time, "14:00");
        assert!(clara_opp.match_score >= 80);

        let still_free_sunday_ten = result
            .unmatched_gaps
            .iter()
            .any(|g| g.date == today && g.start_time == "10:00");
        assert!(
            !still_free_sunday_ten,
            "Alice claimed today 10:00 — not in unmatched"
        );

        assert!(result.unmatched_gaps.len() > 50);
    }

    #[test]
    fn scenario_due_without_gap_when_no_slots_fit() {
        let today = jer_midnight(2026, 4, 19);
        let mut names = HashMap::new();
        names.insert("u1".into(), "Alice".into());

        let cadences = compute_cadences(
            &alice_bookings(),
            &names,
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 90, 2),
        );
        assert_eq!(cadences.len(), 1);

        let all_closed = oh(&[
            ("sunday", closed()),
            ("monday", closed()),
            ("tuesday", closed()),
            ("wednesday", closed()),
            ("thursday", closed()),
            ("friday", closed()),
            ("saturday", closed()),
        ]);
        let empty_gaps = find_gaps(
            &all_closed,
            &[],
            &[],
            &FindGapsParams {
                barber_id: "barber1".into(),
                horizon_days: 14,
                slot_minutes: 30,
                now: today,
                timezone: JERUSALEM,
            },
        );
        assert!(empty_gaps.is_empty());

        let result = match_opportunities(cadences, empty_gaps);
        assert!(result.matched.is_empty());
        assert!(result.unmatched_gaps.is_empty());
        assert_eq!(result.due_without_gap.len(), 1);
        assert_eq!(result.due_without_gap[0].customer.user_id, "u1");
        assert!(result.due_without_gap[0].gap.is_none());
        assert_eq!(result.due_without_gap[0].match_score, 0);
    }

    // =========================================================================
    // DST + weekday-convention parity tests
    // =========================================================================

    #[test]
    fn weekday_convention_zero_sunday_six_saturday() {
        assert_eq!(
            NaiveDate::from_ymd_opt(2026, 4, 19)
                .unwrap()
                .weekday()
                .num_days_from_sunday(),
            0
        );
    }

    #[test]
    fn dst_boundary_does_not_shift_day_of_week() {
        // DST starts last Friday of March 2026 = March 27.
        // Sunday March 22 (pre) and Sunday March 29 (post) must both register DOW=0.
        for (y, mo, d) in [(2026, 3, 22), (2026, 3, 29)] {
            let dt = JERUSALEM
                .with_ymd_and_hms(y, mo, d, 0, 0, 0)
                .single()
                .unwrap();
            let date = dt.date_naive();
            assert_eq!(
                date.weekday().num_days_from_sunday(),
                0,
                "{y}-{mo}-{d} should be Sunday"
            );
        }
    }

    #[test]
    fn dst_crossing_cadence_preserves_weekday() {
        // Sundays straddling DST: 2026-03-22 (pre) and 2026-03-29 (post).
        // Mode should still be Sunday (0).
        let today = jer_midnight(2026, 4, 5);
        let result = compute_cadences(
            &[
                b("u1", jer_midnight(2026, 3, 22)),
                b("u1", jer_midnight(2026, 3, 29)),
            ],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cadence_params(today, 100_000, 2),
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].most_common_day_of_week, 0);
    }

    // =========================================================================
    // Unit tests for internal helpers
    // =========================================================================

    #[test]
    fn median_empty_returns_zero() {
        assert_eq!(median_f64(&[]), 0.0);
    }

    #[test]
    fn bucket_half_hour_edges() {
        assert_eq!(bucket_half_hour("09:00").as_deref(), Some("09:00"));
        assert_eq!(bucket_half_hour("09:29").as_deref(), Some("09:00"));
        assert_eq!(bucket_half_hour("09:30").as_deref(), Some("09:30"));
        assert_eq!(bucket_half_hour("09:59").as_deref(), Some("09:30"));
        assert_eq!(bucket_half_hour("10:05").as_deref(), Some("10:00"));
        assert_eq!(bucket_half_hour(""), None);
        assert_eq!(bucket_half_hour("garbage"), None);
        assert_eq!(bucket_half_hour("9"), None);
    }

    #[test]
    fn parse_and_format_hm_roundtrip() {
        assert_eq!(parse_hm("09:30"), Some(570));
        assert_eq!(parse_hm("00:00"), Some(0));
        assert_eq!(parse_hm("23:59"), Some(23 * 60 + 59));
        assert_eq!(parse_hm(""), None);
        assert_eq!(parse_hm("12"), None);
        assert_eq!(format_hm(570), "09:30");
        assert_eq!(format_hm(0), "00:00");
    }
}
