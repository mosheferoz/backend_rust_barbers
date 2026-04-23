pub mod algorithm;
pub mod cache;
pub mod handlers;
pub mod repository;
pub mod scheduler;
pub mod types;
pub mod vacation_expander;

pub use algorithm::{
    compute_cadences, find_gaps, match_opportunities, CadenceParams, FindGapsParams,
};
pub use types::{
    BookingDoc, CustomerCadence, MatchOpportunitiesResult, OpeningHours, OpeningHoursDay,
    Opportunity, ScheduleGap, TimeBlockDoc,
};
pub use vacation_expander::expand_vacations_and_breaks;
