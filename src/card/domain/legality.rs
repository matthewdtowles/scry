use super::{Format, LegalityStatus};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
pub struct Legality {
    pub card_id: String,
    pub format: Format,
    pub status: LegalityStatus,
}

impl Legality {
    pub fn new(card_id: String, format: Format, status: LegalityStatus) -> Self {
        Self {
            card_id,
            format,
            status,
        }
    }

    /// Only create legality if status is a valid LegalityStatus
    pub fn new_if_relevant(
        card_id: String,
        format: Format,
        status: LegalityStatus,
    ) -> Option<Self> {
        match status {
            LegalityStatus::Legal | LegalityStatus::Banned | LegalityStatus::Restricted => {
                Some(Self::new(card_id, format, status))
            }
        }
    }
}
