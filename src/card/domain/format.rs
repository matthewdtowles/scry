use serde::{Deserialize, Serialize};
use sqlx::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "format_enum")]
#[sqlx(rename_all = "lowercase")]
pub enum Format {
    Standard,
    Commander,
    Modern,
    Legacy,
    Vintage,
    Brawl,
    Explorer,
    Historic,
    Oathbreaker,
    Pauper,
    Pioneer,
}

impl Format {
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Standard => "standard",
            Format::Commander => "commander",
            Format::Modern => "modern",
            Format::Legacy => "legacy",
            Format::Vintage => "vintage",
            Format::Brawl => "brawl",
            Format::Explorer => "explorer",
            Format::Historic => "historic",
            Format::Oathbreaker => "oathbreaker",
            Format::Pauper => "pauper",
            Format::Pioneer => "pioneer",
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "standard" => Ok(Format::Standard),
            "commander" => Ok(Format::Commander),
            "modern" => Ok(Format::Modern),
            "legacy" => Ok(Format::Legacy),
            "vintage" => Ok(Format::Vintage),
            "brawl" => Ok(Format::Brawl),
            "explorer" => Ok(Format::Explorer),
            "historic" => Ok(Format::Historic),
            "oathbreaker" => Ok(Format::Oathbreaker),
            "pauper" => Ok(Format::Pauper),
            "pioneer" => Ok(Format::Pioneer),
            _ => Err(format!("Invalid format: {}", s)),
        }
    }
}
