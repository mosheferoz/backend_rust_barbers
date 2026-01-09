use axum::{
    extract::{State, Json, Path},
    response::IntoResponse,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct ReminderConfig {
    pub enabled: Option<bool>,
    pub immediate_reminder: Option<bool>,
    pub reminder_before_time: Option<String>, // e.g., "15m", "1h", "24h"
    pub remind_at: Option<String>, // ISO8601 datetime
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

// Pulseem API structures - matching exact API schema
#[derive(Serialize)]
struct PulseemSmsClientDetails {
    cellphone: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "firstName")]
    first_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastName")]
    last_name: Option<String>,
}

#[derive(Serialize)]
struct PulseemSendSmsCampaign {
    #[serde(rename = "smsCampaignID")]
    sms_campaign_id: i32,
    #[serde(rename = "isTest")]
    is_test: bool,
    #[serde(skip_serializing_if = "Option::is_none", rename = "groupIds")]
    group_ids: Option<Vec<i32>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "sendTime")]
    send_time: Option<String>, // ISO8601 for scheduled, None for immediate
    #[serde(skip_serializing_if = "Option::is_none", rename = "sendingDetails")]
    sending_details: Option<Vec<PulseemSmsClientDetails>>,
}

#[derive(Serialize)]
struct PulseemCreateSmsCampaign {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "fromNumber")]
    from_number: Option<String>,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "isLinksStatistics")]
    is_links_statistics: Option<bool>,
}

// Helper function to send SMS via Pulseem
async fn send_pulseem_sms(
    api_key: &str,
    phone_number: String,
    message: String,
    send_time: Option<String>, // ISO8601 for scheduled, None for immediate
) -> Result<i32, String> {
    let client = reqwest::Client::new();
    let base_url = "https://ui-api.pulseem.com";

    // Get from number from environment variable (required by Pulseem API)
    let from_number = std::env::var("PULSEEM_FROM_NUMBER")
        .ok()
        .filter(|s| !s.is_empty());
    
    if from_number.is_none() {
        eprintln!("⚠️  Warning: PULSEEM_FROM_NUMBER not set. SMS sending may fail.");
        eprintln!("   Please set PULSEEM_FROM_NUMBER in your .env file (e.g., PULSEEM_FROM_NUMBER=+972501234567)");
    }

    // Step 1: Create SMS campaign
    println!("🔨 Step 1: Creating SMS Campaign via Pulseem API");
    let campaign_name = format!("Reminder_{}", uuid::Uuid::new_v4());
    let create_campaign = PulseemCreateSmsCampaign {
        name: campaign_name.clone(),
        from_number: from_number.clone(),
        text: message.clone(),
        is_links_statistics: Some(false),
    };
    
    if let Some(ref fn_num) = from_number {
        println!("📱 From Number: {}", fn_num);
    } else {
        println!("⚠️  From Number: NOT SET (this may cause the API to fail)");
    }
    
    let create_url = format!("{}/api/v1/SmsCampaignApi/CreateSmsCampaign", base_url);
    println!("📤 POST {}", create_url);
    println!("📦 Request body: {}", serde_json::to_string(&create_campaign).unwrap_or_default());
    
    let create_response = client
        .post(&create_url)
        .header("APIKey", api_key)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&create_campaign)
        .send()
        .await
        .map_err(|e| {
            eprintln!("❌ Failed to send request to Pulseem: {}", e);
            format!("Failed to create SMS campaign: {}", e)
        })?;

    let status = create_response.status();
    println!("📥 Response status: {} {}", status.as_u16(), status.as_str());
    
    // Try to parse response - could be JSON with smsCampaignID or just a number
    let response_text = create_response
        .text()
        .await
        .map_err(|e| format!("Failed to read campaign response: {}", e))?;
    
    println!("📥 Response body: {}", response_text);
    
    // Check if response contains an error
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&response_text) {
        if let Some(status) = json_value.get("status").and_then(|v| v.as_str()) {
            if status == "Error" {
                let error_msg = json_value
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error");
                eprintln!("❌ CreateSmsCampaign failed: {}", error_msg);
                return Err(format!("Failed to create SMS campaign: {}", error_msg));
            }
        }
    }
    
    if !status.is_success() {
        eprintln!("❌ CreateSmsCampaign failed with status {}: {}", status, response_text);
        return Err(format!("Failed to create SMS campaign: {}", response_text));
    }
    
    let campaign_id = if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&response_text) {
        // Try to get smsCampaignID from JSON
        // First check in data.smsCampaignId (Pulseem API format)
        let id = json_value
            .get("data")
            .and_then(|data| data.get("smsCampaignId"))
            .or_else(|| json_value.get("data").and_then(|data| data.get("smsCampaignID")))
            // Fallback to root level
            .or_else(|| json_value.get("smsCampaignID"))
            .or_else(|| json_value.get("smsCampaignId"))
            .or_else(|| json_value.get("campaignId"))
            .or_else(|| json_value.get("id"))
            .and_then(|v| v.as_i64())
            .map(|id| id as i32);
        
        if let Some(id) = id {
            id
        } else {
            eprintln!("❌ No campaign ID found in response. Response structure: {}", response_text);
            return Err(format!("No campaign ID found in response: {}", response_text));
        }
    } else if let Ok(id) = response_text.trim().parse::<i32>() {
        // Response is just a number
        id
    } else {
        eprintln!("❌ Failed to parse campaign ID from response: {}", response_text);
        return Err(format!("Failed to parse campaign ID from response: {}", response_text));
    };
    
    println!("✅ Campaign created successfully! Campaign ID: {}", campaign_id);

    // Step 2: Send SMS campaign
    println!("📨 Step 2: Sending SMS Campaign via Pulseem API");
    let client_details = PulseemSmsClientDetails {
        cellphone: phone_number.clone(),
        first_name: None,
        last_name: None,
    };

    let send_campaign = PulseemSendSmsCampaign {
        sms_campaign_id: campaign_id,
        is_test: false,
        group_ids: None,
        send_time: send_time.clone(),
        sending_details: Some(vec![client_details]),
    };
    
    let send_url = format!("{}/api/v1/SmsCampaignApi/SendSmsCampaign", base_url);
    println!("📤 POST {}", send_url);
    println!("📦 Request body: {}", serde_json::to_string(&send_campaign).unwrap_or_default());
    println!("📱 Sending to: {}", phone_number);
    if let Some(ref st) = send_time {
        println!("⏰ Scheduled for: {}", st);
    } else {
        println!("⚡ Sending immediately");
    }

    let send_response = client
        .post(&send_url)
        .header("APIKey", api_key)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&send_campaign)
        .send()
        .await
        .map_err(|e| {
            eprintln!("❌ Failed to send request to Pulseem: {}", e);
            format!("Failed to send SMS campaign: {}", e)
        })?;

    let send_status = send_response.status();
    println!("📥 Response status: {} {}", send_status.as_u16(), send_status.as_str());
    
    let send_response_text = send_response
        .text()
        .await
        .unwrap_or_default();
    println!("📥 Response body: {}", send_response_text);

    if !send_status.is_success() {
        eprintln!("❌ SendSmsCampaign failed: {}", send_response_text);
        return Err(format!("Failed to send SMS campaign: {}", send_response_text));
    }
    
    println!("✅ SMS sent successfully!");

    Ok(campaign_id)
}

// Helper function to parse reminder time (e.g., "15m", "1h", "24h") to minutes
fn parse_reminder_time(reminder_time: &str) -> Option<i32> {
    let reminder_time = reminder_time.trim().to_lowercase();
    
    if reminder_time.ends_with('m') {
        reminder_time[..reminder_time.len() - 1]
            .parse::<i32>()
            .ok()
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
            appointment_time.parse::<DateTime<chrono::Utc>>()
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
    println!("📥 ===== Received SMS Reminder Request =====");
    println!("📱 Phone: {}", payload.phone_number);
    println!("👤 Customer: {}", payload.customer_name);
    println!("📅 Appointment: {}", payload.appointment_time);
    println!("💇 Barber: {}", payload.barber_name);
    println!("💬 Message: {:?}", payload.message);
    println!("🔔 Reminders: {:?}", payload.reminders);
    println!("===========================================");

    let api_key = std::env::var("PULSEEM_API_KEY")
        .unwrap_or_else(|_| {
            eprintln!("Warning: PULSEEM_API_KEY not set, SMS sending will fail");
            String::new()
        });

    let reminder_id = uuid::Uuid::new_v4().to_string();
    let mut scheduled_reminder_ids = Vec::new();
    let mut sms_sent = false;

    // Build default message if not provided
    let default_message = format!(
        "היי {}, נקבע לך תור ל{} לשעה {}",
        payload.customer_name,
        payload.barber_name,
        payload.appointment_time
    );
    let message = payload.message.as_deref().unwrap_or(&default_message);

    // Process reminders array if provided
    if let Some(reminders) = payload.reminders {
        for reminder in reminders {
            if reminder.enabled.unwrap_or(false) && reminder.reminder_type == "sms" {
                // Immediate reminder
                if reminder.immediate_reminder.unwrap_or(false) {
                    println!("⚡ Processing IMMEDIATE SMS reminder");
                    match send_pulseem_sms(
                        &api_key,
                        payload.phone_number.clone(),
                        message.to_string(),
                        None, // Immediate
                    )
                    .await
                    {
                        Ok(campaign_id) => {
                            println!("✅ Immediate SMS sent successfully! Campaign ID: {}", campaign_id);
                            sms_sent = true;
                            scheduled_reminder_ids.push(campaign_id.to_string());
                        }
                        Err(e) => {
                            eprintln!("❌ Failed to send immediate SMS: {}", e);
                        }
                    }
                }

                // Scheduled reminder (before appointment)
                if let Some(reminder_before_time) = reminder.reminder_before_time {
                    if let Some(send_time) = calculate_reminder_time(&payload.appointment_time, &reminder_before_time) {
                        println!("⏰ Processing SCHEDULED SMS reminder ({} before appointment)", reminder_before_time);
                        println!("📅 Scheduled for: {}", send_time);
                        match send_pulseem_sms(
                            &api_key,
                            payload.phone_number.clone(),
                            message.to_string(),
                            Some(send_time),
                        )
                        .await
                        {
                            Ok(campaign_id) => {
                                println!("✅ Scheduled SMS reminder sent successfully! Campaign ID: {}", campaign_id);
                                scheduled_reminder_ids.push(campaign_id.to_string());
                            }
                            Err(e) => {
                                eprintln!("❌ Failed to send scheduled SMS: {}", e);
                            }
                        }
                    } else {
                        eprintln!("⚠️  Could not calculate reminder time for: {}", reminder_before_time);
                    }
                }

                // Scheduled reminder (at specific time)
                if let Some(remind_at) = reminder.remind_at {
                    println!("⏰ Processing FUTURE SMS reminder at specific time");
                    println!("📅 Scheduled for: {}", remind_at);
                    match send_pulseem_sms(
                        &api_key,
                        payload.phone_number.clone(),
                        message.to_string(),
                        Some(remind_at),
                    )
                    .await
                    {
                        Ok(campaign_id) => {
                            println!("✅ Future SMS reminder sent successfully! Campaign ID: {}", campaign_id);
                            scheduled_reminder_ids.push(campaign_id.to_string());
                        }
                        Err(e) => {
                            eprintln!("❌ Failed to send future SMS: {}", e);
                        }
                    }
                }
            }
        }
    } else {
        // Fallback to old behavior for backward compatibility
        println!("⚠️  Using LEGACY mode (no reminders array provided)");
        if payload.send_immediate.unwrap_or(false) {
            println!("⚡ Processing IMMEDIATE SMS (legacy mode)");
            match send_pulseem_sms(
                &api_key,
                payload.phone_number.clone(),
                message.to_string(),
                None,
            )
            .await
            {
                Ok(campaign_id) => {
                    println!("✅ Immediate SMS sent successfully! Campaign ID: {}", campaign_id);
                    sms_sent = true;
                    scheduled_reminder_ids.push(campaign_id.to_string());
                }
                Err(e) => {
                    eprintln!("❌ Failed to send immediate SMS: {}", e);
                }
            }
        }

        if let Some(reminder_before_minutes) = payload.reminder_before_minutes {
            // Calculate send time
            use chrono::{DateTime, Duration, Utc};
            if let Ok(appointment) = payload.appointment_time.parse::<DateTime<Utc>>() {
                let reminder_time = appointment - Duration::minutes(reminder_before_minutes as i64);
                let send_time = reminder_time.to_rfc3339();
                
                println!("⏰ Processing SCHEDULED SMS (legacy mode) - {} minutes before appointment", reminder_before_minutes);
                println!("📅 Scheduled for: {}", send_time);
                match send_pulseem_sms(
                    &api_key,
                    payload.phone_number.clone(),
                    message.to_string(),
                    Some(send_time),
                )
                .await
                {
                    Ok(campaign_id) => {
                        println!("✅ Scheduled SMS reminder sent successfully! Campaign ID: {}", campaign_id);
                        scheduled_reminder_ids.push(campaign_id.to_string());
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to send scheduled SMS: {}", e);
                    }
                }
            } else {
                eprintln!("❌ Failed to parse appointment time: {}", payload.appointment_time);
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
    if let Err(e) = state.db.fluent()
        .insert()
        .into("reminders")
        .document_id(&reminder_id)
        .object(&reminder_doc)
        .execute::<serde_json::Value>()
        .await
    {
        eprintln!("Warning: Failed to save reminder to Firestore: {}", e);
    }

    println!("📊 ===== Reminder Request Summary =====");
    println!("🆔 Reminder ID: {}", reminder_id);
    println!("✅ SMS Sent: {}", sms_sent);
    println!("📋 Scheduled Reminders: {:?}", scheduled_reminder_ids);
    println!("=======================================");
    
    (StatusCode::CREATED, Json(ScheduleReminderResponse {
        reminder_id,
        status: "scheduled".to_string(),
        sms_sent: Some(sms_sent),
        scheduled_reminders: Some(scheduled_reminder_ids),
    }))
}

pub async fn cancel_reminder(
    Path(id): Path<String>,
    State(_state): State<AppState>,
) -> impl IntoResponse {
    println!("Received cancel reminder request for id: {}", id);

    // TODO: Implement actual cancellation logic
    
    StatusCode::NO_CONTENT
}
