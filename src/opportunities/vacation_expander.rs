use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::Value;

use super::types::TimeBlockDoc;

/// Port of `BarberScheduleController.convertVacationsAndBreaksToTimeBlocks`
/// from `lib/pages/barber_pages/barber_schedule/barber_schedule_controller.dart`.
///
/// Reads `vacationsAndBreaks` from the barber document and expands each entry
/// into per-day `TimeBlockDoc`s that fall inside `[window_start, window_end_exclusive)`.
/// Entries may be full-day ranges (no `startTime`/`endTime`) or recurring
/// break windows (with `startTime..endTime` applied to each day in the range).
pub fn expand_vacations_and_breaks(
    raw_barber_data: &Value,
    _barber_id: &str,
    window_start: NaiveDate,
    window_end_exclusive: NaiveDate,
    tz: Tz,
) -> Vec<TimeBlockDoc> {
    let Some(items) = raw_barber_data
        .get("vacationsAndBreaks")
        .and_then(|v| v.as_array())
    else {
        return Vec::new();
    };

    let mut blocks = Vec::new();

    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };

        let Some(start_date) = parse_date_field(obj.get("startDate"), tz) else {
            continue;
        };
        let Some(end_date) = parse_date_field(obj.get("endDate"), tz) else {
            continue;
        };

        let start_time_str = obj.get("startTime").and_then(|v| v.as_str());
        let end_time_str = obj.get("endTime").and_then(|v| v.as_str());

        let mut cursor = start_date;
        while cursor <= end_date {
            if cursor >= window_start && cursor < window_end_exclusive {
                let (day_start, day_end) = match (start_time_str, end_time_str) {
                    (Some(s), Some(e)) => {
                        let (sh, sm) = parse_hm(s).unwrap_or((0, 0));
                        let (eh, em) = parse_hm(e).unwrap_or((23, 59));
                        (
                            local_to_utc(cursor, sh, sm, tz),
                            local_to_utc(cursor, eh, em, tz),
                        )
                    }
                    _ => (
                        local_to_utc(cursor, 0, 0, tz),
                        local_to_utc(cursor, 23, 59, tz),
                    ),
                };
                blocks.push(TimeBlockDoc {
                    start_time: Some(day_start),
                    end_time: Some(day_end),
                });
            }
            let Some(next) = cursor.succ_opt() else {
                break;
            };
            cursor = next;
        }
    }

    blocks
}

fn parse_date_field(v: Option<&Value>, tz: Tz) -> Option<NaiveDate> {
    let v = v?;
    if v.is_null() {
        return None;
    }

    // ISO-8601 string (e.g. "2026-04-19T00:00:00Z" or "2026-04-19").
    if let Some(s) = v.as_str() {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(dt.with_timezone(&tz).date_naive());
        }
        if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            return Some(d);
        }
        return None;
    }

    // Integer: milliseconds since epoch.
    if let Some(n) = v.as_i64() {
        let secs = n / 1000;
        let nanos = ((n.rem_euclid(1000)) as u32) * 1_000_000;
        if let Some(dt) = Utc.timestamp_opt(secs, nanos).single() {
            return Some(dt.with_timezone(&tz).date_naive());
        }
        return None;
    }

    // Firestore timestamp object: {"_seconds": N, "_nanoseconds": M} or {"seconds":..., "nanoseconds":...}.
    if let Some(obj) = v.as_object() {
        let seconds = obj
            .get("_seconds")
            .or_else(|| obj.get("seconds"))
            .and_then(|x| x.as_i64());
        if let Some(secs) = seconds {
            let nanos = obj
                .get("_nanoseconds")
                .or_else(|| obj.get("nanoseconds"))
                .and_then(|x| x.as_u64())
                .unwrap_or(0) as u32;
            if let Some(dt) = Utc.timestamp_opt(secs, nanos).single() {
                return Some(dt.with_timezone(&tz).date_naive());
            }
        }
    }

    None
}

fn parse_hm(hm: &str) -> Option<(u32, u32)> {
    let mut parts = hm.splitn(3, ':');
    let h_str = parts.next()?;
    let m_str = parts.next()?;
    let h: u32 = h_str.parse().ok()?;
    let m: u32 = m_str.parse().ok()?;
    Some((h, m))
}

fn local_to_utc(date: NaiveDate, h: u32, m: u32, tz: Tz) -> DateTime<Utc> {
    let lr = tz.with_ymd_and_hms(date.year(), date.month(), date.day(), h, m, 0);
    lr.single()
        .or_else(|| lr.earliest())
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;
    use serde_json::json;

    const JERUSALEM: Tz = chrono_tz::Asia::Jerusalem;

    #[test]
    fn empty_barber_data_returns_empty() {
        let blocks = expand_vacations_and_breaks(
            &json!({}),
            "barber1",
            NaiveDate::from_ymd_opt(2026, 4, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
            JERUSALEM,
        );
        assert!(blocks.is_empty());
    }

    #[test]
    fn full_day_vacation_expands_per_day_in_window() {
        let data = json!({
            "vacationsAndBreaks": [
                {
                    "type": "vacation",
                    "startDate": "2026-04-20T00:00:00Z",
                    "endDate":   "2026-04-22T00:00:00Z"
                }
            ]
        });
        let blocks = expand_vacations_and_breaks(
            &data,
            "barber1",
            NaiveDate::from_ymd_opt(2026, 4, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
            JERUSALEM,
        );
        assert_eq!(blocks.len(), 3);
    }

    #[test]
    fn recurring_break_applies_time_window_each_day() {
        let data = json!({
            "vacationsAndBreaks": [
                {
                    "type": "break",
                    "startDate": "2026-04-19T00:00:00Z",
                    "endDate":   "2026-04-20T00:00:00Z",
                    "startTime": "13:00",
                    "endTime":   "14:00"
                }
            ]
        });
        let blocks = expand_vacations_and_breaks(
            &data,
            "barber1",
            NaiveDate::from_ymd_opt(2026, 4, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
            JERUSALEM,
        );
        assert_eq!(blocks.len(), 2);
        for blk in &blocks {
            let start = blk.start_time.unwrap().with_timezone(&JERUSALEM);
            let end = blk.end_time.unwrap().with_timezone(&JERUSALEM);
            assert_eq!(start.hour(), 13);
            assert_eq!(end.hour(), 14);
        }
    }

    #[test]
    fn vacation_outside_window_is_dropped() {
        let data = json!({
            "vacationsAndBreaks": [
                {
                    "type": "vacation",
                    "startDate": "2026-01-01T00:00:00Z",
                    "endDate":   "2026-01-05T00:00:00Z"
                }
            ]
        });
        let blocks = expand_vacations_and_breaks(
            &data,
            "barber1",
            NaiveDate::from_ymd_opt(2026, 4, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
            JERUSALEM,
        );
        assert!(blocks.is_empty());
    }

    #[test]
    fn missing_dates_skip_item() {
        let data = json!({
            "vacationsAndBreaks": [
                {"type": "vacation", "endDate": "2026-04-20T00:00:00Z"},
                {"type": "vacation", "startDate": "2026-04-20T00:00:00Z"},
                {"type": "vacation"}
            ]
        });
        let blocks = expand_vacations_and_breaks(
            &data,
            "barber1",
            NaiveDate::from_ymd_opt(2026, 4, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
            JERUSALEM,
        );
        assert!(blocks.is_empty());
    }

    #[test]
    fn firestore_seconds_timestamp_parses() {
        // Represent 2026-04-21 00:00:00 UTC.
        let ts = 1776988800i64; // 2026-04-21T00:00:00Z
        let data = json!({
            "vacationsAndBreaks": [
                {
                    "type": "vacation",
                    "startDate": {"_seconds": ts, "_nanoseconds": 0},
                    "endDate":   {"_seconds": ts, "_nanoseconds": 0}
                }
            ]
        });
        let blocks = expand_vacations_and_breaks(
            &data,
            "barber1",
            NaiveDate::from_ymd_opt(2026, 4, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
            JERUSALEM,
        );
        assert_eq!(blocks.len(), 1);
    }

    #[test]
    fn ms_since_epoch_numeric_parses() {
        let ms = 1776988800000i64;
        let data = json!({
            "vacationsAndBreaks": [
                {
                    "type": "break",
                    "startDate": ms,
                    "endDate":   ms,
                    "startTime": "12:00",
                    "endTime":   "13:00"
                }
            ]
        });
        let blocks = expand_vacations_and_breaks(
            &data,
            "barber1",
            NaiveDate::from_ymd_opt(2026, 4, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
            JERUSALEM,
        );
        assert_eq!(blocks.len(), 1);
    }
}
