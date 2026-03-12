use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow)]
pub struct PortfolioValueSnapshot {
    pub user_id: i32,
    pub total_value: Decimal,
    pub total_cost: Option<Decimal>,
    pub total_cards: i32,
    pub date: NaiveDate,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_snapshot() -> PortfolioValueSnapshot {
        PortfolioValueSnapshot {
            user_id: 1,
            total_value: Decimal::new(10050, 2), // 100.50
            total_cost: Some(Decimal::new(8000, 2)), // 80.00
            total_cards: 25,
            date: NaiveDate::from_ymd_opt(2025, 6, 1).unwrap(),
        }
    }

    #[test]
    fn test_create_snapshot_with_cost() {
        let snapshot = create_test_snapshot();
        assert_eq!(snapshot.user_id, 1);
        assert_eq!(snapshot.total_value, Decimal::new(10050, 2));
        assert_eq!(snapshot.total_cost, Some(Decimal::new(8000, 2)));
        assert_eq!(snapshot.total_cards, 25);
        assert_eq!(
            snapshot.date,
            NaiveDate::from_ymd_opt(2025, 6, 1).unwrap()
        );
    }

    #[test]
    fn test_create_snapshot_without_cost() {
        let snapshot = PortfolioValueSnapshot {
            user_id: 2,
            total_value: Decimal::new(5000, 2),
            total_cost: None,
            total_cards: 10,
            date: NaiveDate::from_ymd_opt(2025, 6, 1).unwrap(),
        };
        assert_eq!(snapshot.total_cost, None);
        assert_eq!(snapshot.total_value, Decimal::new(5000, 2));
    }

    #[test]
    fn test_snapshot_clone() {
        let original = create_test_snapshot();
        let cloned = original.clone();
        assert_eq!(original.user_id, cloned.user_id);
        assert_eq!(original.total_value, cloned.total_value);
        assert_eq!(original.total_cost, cloned.total_cost);
        assert_eq!(original.total_cards, cloned.total_cards);
        assert_eq!(original.date, cloned.date);
    }

    #[test]
    fn test_snapshot_zero_value() {
        let snapshot = PortfolioValueSnapshot {
            user_id: 1,
            total_value: Decimal::ZERO,
            total_cost: Some(Decimal::ZERO),
            total_cards: 0,
            date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        };
        assert_eq!(snapshot.total_value, Decimal::ZERO);
        assert_eq!(snapshot.total_cards, 0);
    }

    #[test]
    fn test_snapshot_debug_format() {
        let snapshot = create_test_snapshot();
        let debug_str = format!("{:?}", snapshot);
        assert!(debug_str.contains("PortfolioValueSnapshot"));
        assert!(debug_str.contains("user_id: 1"));
    }
}
