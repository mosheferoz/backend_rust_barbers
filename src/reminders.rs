use crate::AppState;
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

// Colored logging helpers
fn log_section(title: &str) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let _ = stdout.set_color(ColorSpec::new().set_fg(Some(Color::Cyan)).set_bold(true));
    println!("\n{}", "=".repeat(50));
    let _ = write!(&mut stdout, "{}", title);
    let _ = stdout.reset();
    println!();
    println!("{}", "=".repeat(50));
}

fn log_success(msg: &str) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let _ = stdout.set_color(ColorSpec::new().set_fg(Some(Color::Green)).set_bold(true));
    let _ = write!(&mut stdout, "[OK] ");
    let _ = stdout.reset();
    println!("{}", msg);
}

fn log_error(msg: &str) {
    let mut stderr = StandardStream::stderr(ColorChoice::Always);
    let _ = stderr.set_color(ColorSpec::new().set_fg(Some(Color::Red)).set_bold(true));
    let _ = write!(&mut stderr, "[ERROR] ");
    let _ = stderr.reset();
    eprintln!("{}", msg);
}

fn log_warning(msg: &str) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let _ = stdout.set_color(ColorSpec::new().set_fg(Some(Color::Yellow)).set_bold(true));
    let _ = write!(&mut stdout, "[WARN] ");
    let _ = stdout.reset();
    println!("{}", msg);
}

fn log_info(msg: &str) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let _ = stdout.set_color(ColorSpec::new().set_fg(Some(Color::Blue)));
    let _ = write!(&mut stdout, "[INFO] ");
    let _ = stdout.reset();
    println!("{}", msg);
}

fn log_skip(msg: &str) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let _ = stdout.set_color(ColorSpec::new().set_fg(Some(Color::Magenta)));
    let _ = write!(&mut stdout, "[SKIP] ");
    let _ = stdout.reset();
    println!("{}", msg);
}

fn log_field(name: &str, value: &str) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let _ = stdout.set_color(ColorSpec::new().set_fg(Some(Color::White)).set_dimmed(true));
    let _ = write!(&mut stdout, "  {} ", name);
    let _ = stdout.reset();
    println!("{}", value);
}

#[derive(Debug, Deserialize)]
pub struct ReminderConfig {
    pub enabled: Option<bool>,
    pub immediate_reminder: Option<bool>,
    pub reminder_before_time: Option<String>, // e.g., "15m", "1h", "24h"
    pub remind_at: Option<String>,            // ISO8601 datetime
    #[serde(rename = "type")]
    pub reminder_type: String, // "sms" or "push"
}

#[derive(Debug, Deserialize)]
pub struct ScheduleReminderRequest {
    pub phone_number: String,
    pub customer_name: String,
    pub appointment_time: String, // ISO8601
    pub barber_name: String,
    pub message: Option<String>,
    pub reminder_before_minutes: Option<i32>,
    pub send_immediate: Option<bool>,
    pub reminders: Option<Vec<ReminderConfig>>, // Array of reminder configurations
}

#[derive(Serialize)]
pub struct ScheduleReminderResponse {
    pub reminder_id: String,
    pub status: String,
    pub sms_sent: Option<bool>,
    pub scheduled_reminders: Option<Vec<String>>, // IDs of scheduled reminders
}

// Pulseem API structures - Direct Send API (v1/SmsApi/SendSms)
#[derive(Serialize)]
struct PulseemSmsSendData {
    #[serde(rename = "fromNumber")]
    from_number: String,
    #[serde(rename = "toNumberList")]
    to_number_list: Vec<String>,
    #[serde(rename = "referenceList")]
    reference_list: Vec<String>,
    #[serde(rename = "textList")]
    text_list: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "sendTime")]
    send_time: Option<String>, // ISO8601 for scheduled, None for immediate
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "isAutomaticUnsubscribeLink"
    )]
    is_automatic_unsubscribe_link: Option<bool>,
}

#[derive(Serialize)]
struct PulseemSendSmsRequest {
    #[serde(rename = "sendId")]
    send_id: String,
    #[serde(rename = "isAsync")]
    is_async: bool,
    #[serde(skip_serializing_if = "Option::is_none", rename = "cbkUrl")]
    cbk_url: Option<String>,
    #[serde(rename = "smsSendData")]
    sms_send_data: PulseemSmsSendData,
}

#[derive(Debug)]
struct SmsSendResult {
    send_id: String,
    sent_successfully: bool,
    error_message: Option<String>,
}

// Helper function to send SMS via Pulseem Direct Send API
async fn send_pulseem_sms(
    api_key: &str,
    phone_number: String,
    message: String,
    send_time: Option<String>, // ISO8601 for scheduled, None for immediate
) -> Result<SmsSendResult, String> {
    let client = reqwest::Client::new();
    let base_url = "https://api.pulseem.com";

    // Get from number from environment variable (required by Pulseem API)
    let from_number = std::env::var("PULSEEM_FROM_NUMBER").map_err(|_| {
        log_error("PULSEEM_FROM_NUMBER is not set.");
        log_info("Hint: set PULSEEM_FROM_NUMBER in your .env (e.g. PULSEEM_FROM_NUMBER=FadeMe).");
        "PULSEEM_FROM_NUMBER environment variable is required".to_string()
    })?;

    // Generate unique send ID
    let send_id = format!("reminder_{}", uuid::Uuid::new_v4());

    // Convert ISO8601 (UTC) to Pulseem format (yyyyMMddHHmmss) in Israel timezone
    let pulseem_send_time = if let Some(ref st) = send_time {
        use chrono::{DateTime, Utc};
        use chrono_tz::Asia::Jerusalem;

        match st.parse::<DateTime<Utc>>() {
            Ok(dt_utc) => {
                // Convert UTC to Israel timezone
                let dt_israel = dt_utc.with_timezone(&Jerusalem);
                log_info(&format!(
                    "Timezone: UTC {} -> Israel {}",
                    dt_utc.format("%Y-%m-%d %H:%M:%S"),
                    dt_israel.format("%Y-%m-%d %H:%M:%S")
                ));
                Some(dt_israel.format("%Y%m%d%H%M%S").to_string())
            }
            Err(e) => {
                log_error(&format!("Failed to parse send time '{}': {}", st, e));
                log_info(
                    "Hint: send_time should be ISO8601 format (e.g., 2026-01-10T06:45:00+00:00)",
                );
                return Ok(SmsSendResult {
                    send_id,
                    sent_successfully: false,
                    error_message: Some(format!("Invalid send_time format: {}", e)),
                });
            }
        }
    } else {
        None
    };

    // Format send time for logging
    let mode = if pulseem_send_time.is_some() {
        "scheduled"
    } else {
        "immediate"
    };

    log_info("Sending SMS via Pulseem Direct API");
    log_field("From:", &from_number);
    log_field("To:", &phone_number);
    log_field("Mode:", mode);
    if let Some(ref st) = pulseem_send_time {
        log_field("Send time:", st);
    }

    // Build request according to Pulseem Direct Send API
    let sms_send_data = PulseemSmsSendData {
        from_number: from_number.clone(),
        to_number_list: vec![phone_number.clone()],
        reference_list: vec![send_id.clone()],
        text_list: vec![message.clone()],
        send_time: pulseem_send_time.clone(),
        is_automatic_unsubscribe_link: Some(false),
    };

    let request = PulseemSendSmsRequest {
        send_id: send_id.clone(),
        is_async: false,
        cbk_url: None,
        sms_send_data,
    };

    let send_url = format!("{}/api/v1/SmsApi/SendSms", base_url);
    log_field("Request:", &format!("POST {}", send_url));

    let response = client
        .post(&send_url)
        .header("APIKey", api_key)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&request)
        .send()
        .await;

    // Handle network errors
    let response = match response {
        Ok(resp) => resp,
        Err(e) => {
            let error_msg = format!("Network error: {}", e);
            log_error(&format!("SendSms request failed: {}", e));
            return Ok(SmsSendResult {
                send_id,
                sent_successfully: false,
                error_message: Some(error_msg),
            });
        }
    };

    let status = response.status();
    log_field(
        "Response:",
        &format!("{} {}", status.as_u16(), status.as_str()),
    );

    let response_text = response.text().await.unwrap_or_default();

    // Check HTTP status
    if !status.is_success() {
        let error_msg = format!("HTTP {}: {}", status.as_u16(), response_text);
        log_error(&format!("SendSms failed: {}", error_msg));
        return Ok(SmsSendResult {
            send_id,
            sent_successfully: false,
            error_message: Some(error_msg),
        });
    }

    // Parse response and check for API-level errors
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&response_text) {
        // Check for error field or status field
        let error_msg = json_value.get("error").and_then(|v| v.as_str());
        let status_str = json_value.get("status").and_then(|v| v.as_str());
        let success_count = json_value
            .get("success")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // If there's an error message or failed status
        if let Some(err) = error_msg {
            if !err.is_empty() && err != "null" {
                log_error(&format!("SendSms API error: {}", err));
                return Ok(SmsSendResult {
                    send_id,
                    sent_successfully: false,
                    error_message: Some(err.to_string()),
                });
            }
        }

        if matches!(status_str, Some("Error") | Some("error")) {
            let msg = error_msg.unwrap_or("Unknown error");
            log_error(&format!("SendSms failed: {}", msg));
            return Ok(SmsSendResult {
                send_id,
                sent_successfully: false,
                error_message: Some(msg.to_string()),
            });
        }

        // Check if any SMS was successfully sent
        if success_count > 0 {
            log_success(&format!("SMS sent successfully. send_id={}", send_id));
            return Ok(SmsSendResult {
                send_id,
                sent_successfully: true,
                error_message: None,
            });
        }
    }

    // If we got here with HTTP 200 but no clear success indicator, treat as sent
    log_success(&format!("SMS accepted by Pulseem. send_id={}", send_id));
    Ok(SmsSendResult {
        send_id,
        sent_successfully: true,
        error_message: None,
    })
}

// Helper function to check if a scheduled time has already passed (using Israel timezone)
fn is_time_in_past(send_time_iso: &str) -> bool {
    use chrono::{DateTime, Utc};
    use chrono_tz::Asia::Jerusalem;

    // Get current time in Israel
    let now_israel = Utc::now().with_timezone(&Jerusalem);

    // Parse the send time
    match send_time_iso.parse::<DateTime<Utc>>() {
        Ok(send_time_utc) => {
            let send_time_israel = send_time_utc.with_timezone(&Jerusalem);
            let is_past = send_time_israel <= now_israel;

            if is_past {
                log_warning(&format!(
                    "Time {} (Israel) has already passed (now: {})",
                    send_time_israel.format("%Y-%m-%d %H:%M:%S"),
                    now_israel.format("%Y-%m-%d %H:%M:%S")
                ));
            }

            is_past
        }
        Err(_) => {
            // If we can't parse, assume it's valid and let Pulseem handle it
            false
        }
    }
}

// Helper function to parse reminder time (e.g., "15m", "1h", "24h") to minutes
fn parse_reminder_time(reminder_time: &str) -> Option<i32> {
    let reminder_time = reminder_time.trim().to_lowercase();

    if reminder_time.ends_with('m') {
        reminder_time[..reminder_time.len() - 1].parse::<i32>().ok()
    } else if reminder_time.ends_with('h') {
        reminder_time[..reminder_time.len() - 1]
            .parse::<i32>()
            .ok()
            .map(|h| h * 60)
    } else if reminder_time.ends_with('d') {
        reminder_time[..reminder_time.len() - 1]
            .parse::<i32>()
            .ok()
            .map(|d| d * 24 * 60)
    } else {
        // Try to parse as minutes directly
        reminder_time.parse::<i32>().ok()
    }
}

// Helper function to calculate reminder time from appointment time and reminder_before_time
fn calculate_reminder_time(appointment_time: &str, reminder_before_time: &str) -> Option<String> {
    use chrono::{DateTime, Duration};

    let appointment = DateTime::parse_from_rfc3339(appointment_time)
        .or_else(|_| {
            // Try ISO8601 format
            appointment_time
                .parse::<DateTime<chrono::Utc>>()
                .map(|dt| dt.with_timezone(&chrono::FixedOffset::east_opt(0).unwrap()))
        })
        .ok()?;

    let minutes = parse_reminder_time(reminder_before_time)?;
    let reminder_time = appointment - Duration::minutes(minutes as i64);

    Some(reminder_time.to_rfc3339())
}

pub async fn schedule_reminder(
    State(state): State<AppState>,
    Json(payload): Json<ScheduleReminderRequest>,
) -> impl IntoResponse {
    log_section("Received SMS reminder request");
    log_field("Phone:", &payload.phone_number);
    log_field("Customer:", &payload.customer_name);
    log_field("Appointment:", &payload.appointment_time);
    log_field("Barber:", &payload.barber_name);
    log_field(
        "Message:",
        payload.message.as_deref().unwrap_or("<not provided>"),
    );
    // Only show reminders count if using new format
    if let Some(reminders) = payload.reminders.as_ref() {
        log_field("Reminders:", &format!("{} configured", reminders.len()));
    }

    let api_key = std::env::var("PULSEEM_API_KEY").unwrap_or_else(|_| {
        eprintln!("Warning: PULSEEM_API_KEY is not set. SMS sending will fail.");
        String::new()
    });

    let reminder_id = uuid::Uuid::new_v4().to_string();
    let mut scheduled_reminder_ids = Vec::new();
    let mut sms_sent = false;

    // Build default message if not provided
    let default_message = format!(
        "היי {}, נקבע לך תור ל{} לשעה {}",
        payload.customer_name, payload.barber_name, payload.appointment_time
    );
    let message = payload.message.as_deref().unwrap_or(&default_message);

    // Process reminders array if provided
    if let Some(reminders) = payload.reminders.as_ref() {
        for reminder in reminders {
            if reminder.enabled.unwrap_or(false) && reminder.reminder_type == "sms" {
                // Immediate reminder
                if reminder.immediate_reminder.unwrap_or(false) {
                    log_info("Reminder: immediate SMS");
                    match send_pulseem_sms(
                        &api_key,
                        payload.phone_number.clone(),
                        message.to_string(),
                        None, // Immediate
                    )
                    .await
                    {
                        Ok(result) => {
                            if result.sent_successfully {
                                log_success(&format!("Sent. send_id={}", result.send_id));
                                sms_sent = true;
                            } else {
                                log_error(&format!(
                                    "Failed. send_id={}, error: {:?}",
                                    result.send_id, result.error_message
                                ));
                            }
                            scheduled_reminder_ids.push(result.send_id);
                        }
                        Err(e) => {
                            log_error(&format!("Immediate SMS failed: {}", e));
                        }
                    }
                }

                // Scheduled reminder (before appointment)
                if let Some(reminder_before_time) = reminder.reminder_before_time.as_deref() {
                    if let Some(send_time) =
                        calculate_reminder_time(&payload.appointment_time, &reminder_before_time)
                    {
                        log_info(&format!(
                            "Reminder: scheduled SMS (offset: {})",
                            reminder_before_time
                        ));
                        log_field("Send time:", &send_time);

                        // Check if the scheduled time has already passed
                        if is_time_in_past(&send_time) {
                            log_skip("Scheduled time already passed");
                        } else {
                            match send_pulseem_sms(
                                &api_key,
                                payload.phone_number.clone(),
                                message.to_string(),
                                Some(send_time),
                            )
                            .await
                            {
                                Ok(result) => {
                                    if result.sent_successfully {
                                        log_success(&format!(
                                            "Scheduled. send_id={}",
                                            result.send_id
                                        ));
                                    } else {
                                        log_error(&format!(
                                            "Failed. send_id={}, error: {:?}",
                                            result.send_id, result.error_message
                                        ));
                                    }
                                    scheduled_reminder_ids.push(result.send_id);
                                }
                                Err(e) => {
                                    log_error(&format!("Scheduled SMS failed: {}", e));
                                }
                            }
                        }
                    } else {
                        log_error(&format!(
                            "Could not calculate reminder time (offset: {}).",
                            reminder_before_time
                        ));
                    }
                }

                // Scheduled reminder (at specific time)
                if let Some(remind_at) = reminder.remind_at.as_deref() {
                    log_info("Reminder: scheduled SMS (absolute time)");
                    log_field("Send time:", remind_at);

                    // Check if the scheduled time has already passed
                    if is_time_in_past(remind_at) {
                        log_skip("Scheduled time already passed");
                    } else {
                        match send_pulseem_sms(
                            &api_key,
                            payload.phone_number.clone(),
                            message.to_string(),
                            Some(remind_at.to_string()),
                        )
                        .await
                        {
                            Ok(result) => {
                                if result.sent_successfully {
                                    log_success(&format!("Scheduled. send_id={}", result.send_id));
                                } else {
                                    log_error(&format!(
                                        "Failed. send_id={}, error: {:?}",
                                        result.send_id, result.error_message
                                    ));
                                }
                                scheduled_reminder_ids.push(result.send_id);
                            }
                            Err(e) => {
                                log_error(&format!("Future scheduled SMS failed: {}", e));
                            }
                        }
                    }
                }
            }
        }
    } else {
        // Fallback to old behavior for backward compatibility
        if payload.send_immediate.unwrap_or(false) {
            log_info("Sending immediate SMS");
            match send_pulseem_sms(
                &api_key,
                payload.phone_number.clone(),
                message.to_string(),
                None,
            )
            .await
            {
                Ok(result) => {
                    if result.sent_successfully {
                        log_success(&format!("Sent. send_id={}", result.send_id));
                        sms_sent = true;
                    } else {
                        log_error(&format!(
                            "Failed. send_id={}, error: {:?}",
                            result.send_id, result.error_message
                        ));
                    }
                    scheduled_reminder_ids.push(result.send_id);
                }
                Err(e) => {
                    log_error(&format!("Immediate SMS failed: {}", e));
                }
            }
        }

        if let Some(reminder_before_minutes) = payload.reminder_before_minutes {
            // Calculate send time
            use chrono::{DateTime, Duration, Utc};
            if let Ok(appointment) = payload.appointment_time.parse::<DateTime<Utc>>() {
                let reminder_time = appointment - Duration::minutes(reminder_before_minutes as i64);
                let send_time = reminder_time.to_rfc3339();

                // Convert minutes to human readable format
                let time_str = if reminder_before_minutes >= 1440 {
                    format!("{}h", reminder_before_minutes / 60)
                } else if reminder_before_minutes >= 60 {
                    format!("{}h", reminder_before_minutes / 60)
                } else {
                    format!("{}m", reminder_before_minutes)
                };
                log_info(&format!("Scheduling SMS ({} before)", time_str));
                log_field("Send time:", &send_time);

                // Check if the scheduled time has already passed
                if is_time_in_past(&send_time) {
                    log_skip("Scheduled time already passed (immediate message was sent)");
                } else {
                    match send_pulseem_sms(
                        &api_key,
                        payload.phone_number.clone(),
                        message.to_string(),
                        Some(send_time),
                    )
                    .await
                    {
                        Ok(result) => {
                            if result.sent_successfully {
                                log_success(&format!("Scheduled. send_id={}", result.send_id));
                            } else {
                                log_error(&format!(
                                    "Failed. send_id={}, error: {:?}",
                                    result.send_id, result.error_message
                                ));
                            }
                            scheduled_reminder_ids.push(result.send_id);
                        }
                        Err(e) => {
                            log_error(&format!("Scheduled SMS failed: {}", e));
                        }
                    }
                }
            } else {
                log_error(&format!(
                    "Failed to parse appointment time: {}",
                    payload.appointment_time
                ));
            }
        }
    }

    // Save reminder to Firestore for tracking
    let reminder_doc: serde_json::Value = serde_json::json!({
        "reminder_id": reminder_id,
        "phone_number": payload.phone_number,
        "customer_name": payload.customer_name,
        "appointment_time": payload.appointment_time,
        "barber_name": payload.barber_name,
        "message": message,
        "status": "scheduled",
        "sms_sent": sms_sent,
        "scheduled_reminder_ids": scheduled_reminder_ids.clone(),
        "created_at": chrono::Utc::now().to_rfc3339(),
    });

    // Save to Firestore (optional, for tracking)
    if let Err(e) = state
        .db
        .fluent()
        .insert()
        .into("reminders")
        .document_id(&reminder_id)
        .object(&reminder_doc)
        .execute::<serde_json::Value>()
        .await
    {
        log_warning(&format!("Failed to save reminder to Firestore: {}", e));
    }

    log_section("Reminder request summary");
    log_field("reminder_id:", &reminder_id);
    if sms_sent {
        log_success(&format!("sms_sent: {}", sms_sent));
    } else {
        log_field("sms_sent:", &sms_sent.to_string());
    }
    log_field("scheduled_ids:", &format!("{:?}", scheduled_reminder_ids));

    (
        StatusCode::CREATED,
        Json(ScheduleReminderResponse {
            reminder_id,
            status: "scheduled".to_string(),
            sms_sent: Some(sms_sent),
            scheduled_reminders: Some(scheduled_reminder_ids),
        }),
    )
}

pub async fn cancel_reminder(
    Path(id): Path<String>,
    State(_state): State<AppState>,
) -> impl IntoResponse {
    log_info(&format!("Received cancel reminder request. id={}", id));

    // TODO: Implement actual cancellation logic

    StatusCode::NO_CONTENT
}
