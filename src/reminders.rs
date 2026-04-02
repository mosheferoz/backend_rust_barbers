use crate::fcm;
use crate::AppState;
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use chrono_tz::Asia::Jerusalem;
use firestore::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::time::Duration;
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
    #[allow(dead_code)]
    pub reminder_before_time: Option<String>, // e.g., "15m", "1h", "24h" - kept for API compat
    #[allow(dead_code)]
    pub remind_at: Option<String>, // ISO8601 datetime - kept for API compat
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
    #[allow(dead_code)]
    pub reminder_before_minutes: Option<i32>, // kept for API compat, scheduling now in scheduler
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

                // Scheduled reminders (reminder_before_time, remind_at) are now handled
                // by the Rust scheduler via scheduled_reminders collection
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

        // Scheduled SMS (reminder_before_minutes) is now handled by the scheduler
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

/// Background scheduler: runs every 60 seconds, queries scheduled_reminders,
/// sends immediate SMS for due reminders if booking status != "cancelled".
pub async fn start_reminder_scheduler(state: AppState) {
    log_info("Reminder scheduler started (runs every 60 seconds)");
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.tick().await; // First tick completes immediately, skip

    loop {
        interval.tick().await;

        let now_run = Utc::now().with_timezone(&Jerusalem);
        log_info(&format!(
            "[Scheduler] Run at {} (Israel) - checking scheduled_reminders",
            now_run.format("%Y-%m-%d %H:%M:%S")
        ));

        let api_key = std::env::var("PULSEEM_API_KEY").ok();
        let project_id = std::env::var("PROJECT_ID").ok();

        let now_utc = Utc::now();
        let now_israel = now_utc.with_timezone(&Jerusalem);
        let now_plus_one_min = now_utc + chrono::Duration::minutes(1);

        // --- SMS reminders ---
        if let Some(ref api_key) = api_key {
            let docs: Vec<serde_json::Value> = match state
                .db
                .fluent()
                .select()
                .from("scheduled_reminders")
                .filter(|q| {
                    q.for_all([
                        q.field("status").eq("pending"),
                        q.field("type").eq("sms"),
                    ])
                })
                .obj()
                .stream_query_with_errors()
                .await
            {
                Ok(stream) => stream
                    .filter_map(|res| futures::future::ready(res.ok()))
                    .collect()
                    .await,
                Err(e) => {
                    log_error(&format!("Scheduler SMS query failed: {}", e));
                    Vec::new()
                }
            };

            log_info(&format!(
            "[Scheduler] Query returned {} pending SMS reminders",
            docs.len()
        ));

        let mut due_count = 0;
        for doc in &docs {
            let doc_map = match doc.as_object() {
                Some(m) => m,
                None => continue,
            };
            let scheduled_send_time = doc_map
                .get("scheduledSendTime")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if let Ok(send_time_utc) = scheduled_send_time.parse::<DateTime<Utc>>() {
                if send_time_utc <= now_plus_one_min
                    && send_time_utc >= now_utc - chrono::Duration::minutes(2)
                {
                    due_count += 1;
                }
            }
        }
        if due_count > 0 {
            log_info(&format!("[Scheduler] {} reminders due for sending", due_count));
        }

        for doc in docs {
            let doc_map = match doc.as_object() {
                Some(m) => m,
                None => continue,
            };

            let scheduled_send_time = doc_map
                .get("scheduledSendTime")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let doc_id = doc_map.get("id").and_then(|v| v.as_str());

            // Parse scheduled time - must be due (<= now + 1 min)
            let send_time_utc: DateTime<Utc> = match scheduled_send_time.parse() {
                Ok(dt) => dt,
                Err(_) => continue,
            };

            if send_time_utc > now_plus_one_min {
                continue; // Not yet due
            }
            if send_time_utc < now_utc - chrono::Duration::minutes(2) {
                continue; // Too old, might have been missed - skip to avoid duplicates
            }

            let booking_id = doc_map
                .get("bookingId")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let phone = doc_map
                .get("phoneNumber")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let message = doc_map
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if booking_id.is_empty() || phone.is_empty() {
                continue;
            }

            // Check booking status
            let booking: FirestoreResult<Option<serde_json::Value>> = state
                .db
                .fluent()
                .select()
                .by_id_in("bookings")
                .obj()
                .one(booking_id)
                .await;

            let status = booking
                .ok()
                .flatten()
                .and_then(|b| b.get("status").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .unwrap_or_default();

            if status == "cancelled" {
                log_skip(&format!(
                    "Booking {} cancelled, skipping reminder",
                    booking_id
                ));
                continue;
            }

            log_info(&format!(
                "Scheduler: sending SMS for booking {} (due {})",
                booking_id,
                now_israel.format("%Y-%m-%d %H:%M")
            ));

            match send_pulseem_sms(&api_key, phone.to_string(), message.to_string(), None).await
            {
                Ok(result) => {
                    if result.sent_successfully {
                        log_success(&format!("SMS sent for booking {}", booking_id));
                        if let Some(id) = doc_id {
                            let mut updated = doc.clone();
                            if let Some(obj) = updated.as_object_mut() {
                                obj.insert(
                                    "status".to_string(),
                                    serde_json::Value::String("sent".to_string()),
                                );
                                obj.insert(
                                    "sentAt".to_string(),
                                    serde_json::Value::String(
                                        chrono::Utc::now().to_rfc3339(),
                                    ),
                                );
                                if let Err(e) = state
                                    .db
                                    .fluent()
                                    .update()
                                    .in_col("scheduled_reminders")
                                    .document_id(id)
                                    .object(&updated)
                                    .execute::<serde_json::Value>()
                                    .await
                                {
                                    log_warning(&format!(
                                        "Failed to update scheduled_reminder {}: {}",
                                        id, e
                                    ));
                                }
                            }
                        }
                    } else {
                        log_error(&format!(
                            "SMS failed for booking {}: {:?}",
                            booking_id, result.error_message
                        ));
                    }
                }
                Err(e) => {
                    log_error(&format!("SMS send error for booking {}: {}", booking_id, e));
                }
            }
        }
        }

        // --- Push reminders (customer) ---
        if let Some(ref project_id) = project_id {
            let push_docs: Vec<serde_json::Value> = match state
                .db
                .fluent()
                .select()
                .from("scheduled_reminders")
                .filter(|q| {
                    q.for_all([
                        q.field("status").eq("pending"),
                        q.field("type").eq("push"),
                    ])
                })
                .obj()
                .stream_query_with_errors()
                .await
            {
                Ok(stream) => stream
                    .filter_map(|res| futures::future::ready(res.ok()))
                    .collect()
                    .await,
                Err(e) => {
                    log_error(&format!("Scheduler push query failed: {}", e));
                    Vec::new()
                }
            };

            for doc in push_docs {
                let doc_map = match doc.as_object() {
                    Some(m) => m,
                    None => continue,
                };
                let scheduled_send_time = doc_map
                    .get("scheduledSendTime")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let doc_id = doc_map.get("id").and_then(|v| v.as_str());
                let booking_id = doc_map
                    .get("bookingId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let send_time_utc: DateTime<Utc> = match scheduled_send_time.parse() {
                    Ok(dt) => dt,
                    Err(_) => continue,
                };
                if send_time_utc > now_utc + chrono::Duration::minutes(1) {
                    continue;
                }
                if send_time_utc < now_utc - chrono::Duration::minutes(2) {
                    continue;
                }

                if !booking_id.is_empty() {
                    let booking: FirestoreResult<Option<serde_json::Value>> = state
                        .db
                        .fluent()
                        .select()
                        .by_id_in("bookings")
                        .obj()
                        .one(booking_id)
                        .await;
                    let status = booking
                        .ok()
                        .flatten()
                        .and_then(|b| b.get("status").and_then(|v| v.as_str()).map(|s| s.to_string()))
                        .unwrap_or_default();
                    if status == "cancelled" {
                        log_skip(&format!("Booking {} cancelled, skipping push reminder", booking_id));
                        continue;
                    }
                }

                let mut fcm_token = doc_map
                    .get("fcmToken")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if fcm_token.is_empty() {
                    let user_id = doc_map.get("userId").and_then(|v| v.as_str()).unwrap_or("");
                    if !user_id.is_empty() {
                        let user_doc: FirestoreResult<Option<serde_json::Value>> = state
                            .db
                            .fluent()
                            .select()
                            .by_id_in("users")
                            .obj()
                            .one(user_id)
                            .await;
                        if let Ok(Some(ud)) = user_doc {
                            fcm_token = ud
                                .get("fcmToken")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                        }
                    }
                }

                let title = doc_map
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("תזכורת תור");
                let body = doc_map
                    .get("body")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if fcm_token.is_empty() {
                    log_skip("Push reminder: no fcmToken, skipping");
                    continue;
                }

                log_info(&format!(
                    "Scheduler: sending push for booking {} (due {})",
                    booking_id,
                    now_israel.format("%Y-%m-%d %H:%M")
                ));

                let mut push_data = std::collections::HashMap::new();
                push_data.insert("type".to_string(), "reminder".to_string());
                push_data.insert("bookingId".to_string(), booking_id.to_string());

                match fcm::send_fcm_push(project_id, &fcm_token, title, body, Some(push_data)).await {
                    Ok(_) => {
                        log_success(&format!("Push sent for booking {}", booking_id));
                        if let Some(id) = doc_id {
                            let mut updated = doc.clone();
                            if let Some(obj) = updated.as_object_mut() {
                                obj.insert(
                                    "status".to_string(),
                                    serde_json::Value::String("sent".to_string()),
                                );
                                obj.insert(
                                    "sentAt".to_string(),
                                    serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
                                );
                                let _ = state
                                    .db
                                    .fluent()
                                    .update()
                                    .in_col("scheduled_reminders")
                                    .document_id(id)
                                    .object(&updated)
                                    .execute::<serde_json::Value>()
                                    .await;
                            }
                        }
                    }
                    Err(e) => {
                        log_error(&format!("Push failed for booking {}: {}", booking_id, e));
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct NotifyNewBookingRequest {
    pub barber_uid: String,
    pub customer_name: Option<String>,
    pub appointment_date: String,
    pub appointment_time: Option<String>,
    pub services: Option<String>,
    pub booking_id: Option<String>,
}

/// POST /notify-new-booking: send push to barber when a new appointment is booked.
pub async fn notify_new_booking(
    State(state): State<AppState>,
    Json(payload): Json<NotifyNewBookingRequest>,
) -> impl IntoResponse {
    log_section("Notify new booking (push to barber)");
    log_field("barber_uid:", &payload.barber_uid);
    log_field("appointment_date:", &payload.appointment_date);

    if payload.barber_uid.is_empty() {
        log_warning("barber_uid is empty");
        return (StatusCode::BAD_REQUEST, ());
    }

    let barber_doc: FirestoreResult<Option<serde_json::Value>> = state
        .db
        .fluent()
        .select()
        .by_id_in("barbers")
        .obj()
        .one(&payload.barber_uid)
        .await;

    let fcm_token = match barber_doc {
        Ok(Some(doc)) => doc
            .get("fcmToken")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    };

    if fcm_token.is_empty() {
        log_skip("No fcmToken for barber, skipping push");
        return (StatusCode::NOT_FOUND, ());
    }

    let project_id = match std::env::var("PROJECT_ID") {
        Ok(id) if !id.is_empty() => id,
        _ => {
            log_error("PROJECT_ID is not set");
            return (StatusCode::INTERNAL_SERVER_ERROR, ());
        }
    };

    let title = "תור חדש";
    let body = {
        let name = payload.customer_name.as_deref().unwrap_or("").trim();
        let date = &payload.appointment_date;
        let time = payload.appointment_time.as_deref().unwrap_or("").trim();
        let services = payload.services.as_deref().unwrap_or("").trim();

        let mut parts = Vec::new();
        if !name.is_empty() {
            parts.push(name.to_string());
        }
        if !time.is_empty() {
            parts.push(format!("{} בשעה {}", date, time));
        } else {
            parts.push(date.to_string());
        }
        if !services.is_empty() {
            parts.push(services.to_string());
        }
        parts.join(" | ")
    };

    let mut data = std::collections::HashMap::new();
    data.insert("type".to_string(), "new_booking".to_string());
    if let Some(ref bid) = payload.booking_id {
        data.insert("bookingId".to_string(), bid.clone());
    }

    match fcm::send_fcm_push(&project_id, &fcm_token, &title, &body, Some(data)).await {
        Ok(msg_id) => {
            log_success(&format!("Push sent to barber. message_id={}", msg_id));
            (StatusCode::OK, ())
        }
        Err(e) => {
            log_error(&format!("FCM push failed: {}", e));
            (StatusCode::INTERNAL_SERVER_ERROR, ())
        }
    }
}

pub async fn cancel_reminder(
    Path(id): Path<String>,
    State(_state): State<AppState>,
) -> impl IntoResponse {
    log_info(&format!("Received cancel reminder request. id={}", id));

    // Cancellation is now handled via scheduled_reminders status (Flutter updates on cancel)

    StatusCode::NO_CONTENT
}
