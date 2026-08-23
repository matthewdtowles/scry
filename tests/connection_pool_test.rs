use scry::config::Config;
use scry::database::ConnectionPool;

/// Every pooled connection must come back carrying the timeouts, or the whole
/// point of them is lost.
///
/// This is not a formality. The timeouts are applied by an `after_connect` hook
/// that sends `SET statement_timeout = ...; SET lock_timeout = ...; SET
/// idle_in_transaction_session_timeout = ...` as one string. Multi-statement
/// query text is only legal over Postgres's *simple* query protocol, which is
/// what sqlx uses for a query with no bind arguments - and nothing in the type
/// system holds it to that. If that ever changed, or the hook were rewritten to
/// bind a parameter, the connection would fail outright and scry could not open
/// a connection at all.
///
/// Asserting all three settings, rather than just that connecting worked, is
/// deliberate: it proves the last statement in the batch ran, so a silently
/// truncated batch fails here instead of in production.
#[tokio::test]
#[ignore]
async fn pooled_connections_carry_the_configured_timeouts() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("TEST_DATABASE_URL or DATABASE_URL must be set");

    let config = Config {
        database_url,
        max_pool_size: 2,
        statement_timeout_ms: 61_000,
        lock_timeout_ms: 31_000,
        command_timeout_seconds: 1_800,
    };
    let pool = ConnectionPool::new(&config)
        .await
        .expect("after_connect must not reject the connection");

    // pg_settings reports these in milliseconds.
    let setting =
        |name: &str| format!("SELECT setting::bigint FROM pg_settings WHERE name = '{name}'");

    assert_eq!(
        pool.scalar_i64(&setting("statement_timeout"))
            .await
            .unwrap(),
        61_000,
        "statement_timeout did not reach the connection"
    );
    assert_eq!(
        pool.scalar_i64(&setting("lock_timeout")).await.unwrap(),
        31_000,
        "lock_timeout did not reach the connection"
    );
    assert_eq!(
        pool.scalar_i64(&setting("idle_in_transaction_session_timeout"))
            .await
            .unwrap(),
        61_000,
        "idle_in_transaction_session_timeout did not reach the connection - \
         the last statement of the batch was dropped"
    );
}
