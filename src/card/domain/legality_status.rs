use serde::{Deserialize, Serialize};
use sqlx::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "legality_status_enum")]
#[sqlx(rename_all = "lowercase")]
pub enum LegalityStatus {
    Legal,
    Banned,
    Restricted,
}

impl LegalityStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            LegalityStatus::Legal => "legal",
            LegalityStatus::Banned => "banned",
            LegalityStatus::Restricted => "restricted",
        }
    }
}

impl std::fmt::Display for LegalityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for LegalityStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "legal" => Ok(LegalityStatus::Legal),
            "banned" => Ok(LegalityStatus::Banned),
            "restricted" => Ok(LegalityStatus::Restricted),
            _ => Err(format!("Invalid legality status: {}", s)),
        }
    }
}
