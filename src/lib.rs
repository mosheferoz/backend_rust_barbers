use firestore::FirestoreDb;

pub mod auth;
pub mod fcm;
pub mod opportunities;
pub mod reminders;
pub mod team;

#[derive(Clone)]
pub struct AppState {
    pub db: FirestoreDb,
    pub opp_stats: opportunities::scheduler::SharedStats,
}
