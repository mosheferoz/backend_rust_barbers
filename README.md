# Rust Firestore Backend

This is a backend service built with Rust using the `firestore` crate (v0.47+).

## Prerequisites

- Rust (latest stable)
- Google Cloud Project with Firestore enabled

## Configuration

1. Set the `PROJECT_ID` environment variable. You can use a local env file (see `env.example`):
   ```
   PROJECT_ID=your-project-id
   ```
2. Set the `PULSEEM_API_KEY` environment variable for SMS functionality:
   ```
   PULSEEM_API_KEY=your-pulseem-api-key
   ```
   To get your API key, contact Pulseem support at support@pulseem.com

3. Set the `PULSEEM_FROM_NUMBER` environment variable (required for SMS sending):
   ```
   PULSEEM_FROM_NUMBER=+972501234567
   ```
   This is the phone number that will appear as the sender of the SMS messages.
   Contact Pulseem support to get your approved sender number.
3. Authentication:
   - If running locally with gcloud installed: `gcloud auth application-default login`
   - Or set `GOOGLE_APPLICATION_CREDENTIALS` to your service account key file path.

## Running

```bash
cargo run
```

The server will start on `http://127.0.0.1:8080`.

## HTTPS (Production)

הדרך המומלצת היא **לא** להוסיף TLS בתוך ה־Rust, אלא לסיים HTTPS ב־Nginx ולהעביר ל־Rust ב־HTTP על `127.0.0.1:8080`.

- דוגמת קונפיג: `deploy/nginx_eventlyil.xyz.conf`
- דוגמת systemd service: `deploy/rust_backend.service`

### CORS (אם אתה קורא מהדפדפן)

אם הלקוח הוא Web (ולא Mobile), צריך להגדיר CORS. יש תמיכה דרך env:

```bash
CORS_ALLOWED_ORIGINS=https://eventlyil.xyz,https://www.eventlyil.xyz
```

## Endpoints

- `GET /` - Health check
- `GET /barbers` - List barbers
- `POST /barbers` - Create a barber (JSON body required)
- `GET /barbers/:id` - Get a barber by ID
- `POST /schedule-reminder` - Schedule SMS reminders (requires authentication)
- `DELETE /cancel-reminder/:id` - Cancel a scheduled reminder (requires authentication)

### Schedule Reminder Endpoint

The `/schedule-reminder` endpoint accepts a JSON body with the following structure:

```json
{
  "phone_number": "+972501234567",
  "customer_name": "John Doe",
  "appointment_time": "2026-01-15T14:00:00Z",
  "barber_name": "Barber Shop",
  "message": "Optional custom message",
  "reminders": [
    {
      "enabled": true,
      "immediate_reminder": true,
      "reminder_before_time": "15m",
      "type": "sms"
    },
    {
      "enabled": true,
      "remind_at": "2026-01-15T13:45:00.000Z",
      "type": "push"
    }
  ]
}
```

**Reminder Configuration:**
- `enabled`: Whether the reminder is enabled (default: false)
- `immediate_reminder`: Send SMS immediately when booking is created (default: false)
- `reminder_before_time`: Time before appointment to send reminder (e.g., "15m", "1h", "24h")
- `remind_at`: Specific ISO8601 datetime to send reminder
- `type`: Type of reminder - "sms" or "push"

**Response:**
```json
{
  "reminder_id": "uuid",
  "status": "scheduled",
  "sms_sent": true,
  "scheduled_reminders": ["campaign_id_1", "campaign_id_2"]
}
```

The endpoint integrates with Pulseem API to send SMS reminders. It supports:
- Immediate SMS sending
- Scheduled SMS reminders (before appointment)
- Future reminders at specific times





# backend_rust_barbers
