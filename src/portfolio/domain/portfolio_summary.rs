use rust_decimal::Decimal;
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow)]
pub struct PortfolioSummaryRow {
    pub user_id: i32,
    pub total_value: Decimal,
    pub total_cost: Option<Decimal>,
    pub total_realized_gain: Option<Decimal>,
    pub total_cards: i32,
    pub total_quantity: i32,
}

#[derive(Clone, Debug, FromRow)]
pub struct CardPerformanceRow {
    pub user_id: i32,
    pub card_id: String,
    pub is_foil: bool,
    pub quantity: i32,
    pub total_cost: Decimal,
    pub average_cost: Decimal,
    pub current_value: Decimal,
    pub unrealized_gain: Decimal,
    pub realized_gain: Decimal,
    pub roi_percent: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_row() {
        let row = PortfolioSummaryRow {
            user_id: 1,
            total_value: Decimal::new(15000, 2),
            total_cost: Some(Decimal::new(10000, 2)),
            total_realized_gain: Some(Decimal::new(500, 2)),
            total_cards: 10,
            total_quantity: 25,
        };
        assert_eq!(row.user_id, 1);
        assert_eq!(row.total_value, Decimal::new(15000, 2));
        assert_eq!(row.total_cards, 10);
    }

    #[test]
    fn test_summary_row_no_transactions() {
        let row = PortfolioSummaryRow {
            user_id: 2,
            total_value: Decimal::new(5000, 2),
            total_cost: None,
            total_realized_gain: None,
            total_cards: 5,
            total_quantity: 10,
        };
        assert!(row.total_cost.is_none());
        assert!(row.total_realized_gain.is_none());
    }

    #[test]
    fn test_card_performance_row() {
        let row = CardPerformanceRow {
            user_id: 1,
            card_id: "card-123".to_string(),
            is_foil: false,
            quantity: 3,
            total_cost: Decimal::new(1500, 2),
            average_cost: Decimal::new(500, 2),
            current_value: Decimal::new(2100, 2),
            unrealized_gain: Decimal::new(600, 2),
            realized_gain: Decimal::new(200, 2),
            roi_percent: Some(Decimal::new(5333, 2)),
        };
        assert_eq!(row.card_id, "card-123");
        assert_eq!(row.quantity, 3);
        assert!(!row.is_foil);
    }

    #[test]
    fn test_card_performance_foil() {
        let row = CardPerformanceRow {
            user_id: 1,
            card_id: "card-456".to_string(),
            is_foil: true,
            quantity: 1,
            total_cost: Decimal::new(800, 2),
            average_cost: Decimal::new(800, 2),
            current_value: Decimal::new(600, 2),
            unrealized_gain: Decimal::new(-200, 2),
            realized_gain: Decimal::ZERO,
            roi_percent: Some(Decimal::new(-2500, 2)),
        };
        assert!(row.is_foil);
        assert!(row.unrealized_gain < Decimal::ZERO);
    }
}
