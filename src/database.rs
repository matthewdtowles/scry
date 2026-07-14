use crate::config::Config;
use anyhow::Result;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    FromRow, PgPool, Postgres, QueryBuilder, Row,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct ConnectionPool {
    pool: Arc<PgPool>,
}

impl ConnectionPool {
    pub async fn new(config: &Config) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_pool_size)
            .connect(&config.database_url)
            .await?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    pub async fn execute_query_builder(
        &self,
        mut builder: QueryBuilder<'_, sqlx::Postgres>,
    ) -> Result<i64> {
        let result = builder.build().execute(&*self.pool).await?;
        Ok(result.rows_affected() as i64)
    }

    /// Execute several statements in one transaction, in order, returning the
    /// total rows affected. Any failure rolls the whole batch back. Use this
    /// where a multi-statement write must be atomic (e.g. the DELETE+INSERT in
    /// `save_legalities`, or the child-row DELETE+INSERT in `save_deck`; the
    /// deck upsert stays separate because it needs its RETURNING id first).
    pub async fn execute_query_builders_tx(
        &self,
        builders: Vec<QueryBuilder<'_, Postgres>>,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let mut total: i64 = 0;
        for mut builder in builders {
            let result = builder.build().execute(&mut *tx).await?;
            total += result.rows_affected() as i64;
        }
        tx.commit().await?;
        Ok(total)
    }

    /// Run a `SELECT COUNT(...)`-shaped query and return the first column.
    ///
    /// `query` is always a trusted, caller-owned constant (or a query built from
    /// constants) - never user input. Value parameters must be bound via a
    /// `QueryBuilder`, not interpolated.
    pub async fn count(&self, query: &str) -> Result<i64> {
        let row = sqlx::query(query).fetch_one(&*self.pool).await?;
        let count: i64 = row.get(0);
        Ok(count)
    }

    pub async fn fetch_all_query_builder<T>(
        &self,
        mut query_builder: QueryBuilder<'_, Postgres>,
    ) -> Result<Vec<T>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        let query = query_builder.build_query_as::<T>();
        query
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(Into::into)
    }

    /// Execute one or more statements with no bind parameters (fixed DDL /
    /// `TRUNCATE`). `query` is a trusted constant only - it interpolates
    /// nothing, so never route caller input through here.
    pub async fn execute_raw(&self, query: &str) -> Result<()> {
        sqlx::raw_sql(query).execute(&*self.pool).await?;
        Ok(())
    }

    /// `SELECT EXISTS(...)` with a single bound value. `query` is a trusted
    /// constant with its identifiers baked in and the value bound to `$1`, so
    /// there is no identifier interpolation to abuse.
    pub async fn exists(&self, query: &str, value: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(query)
            .bind(value)
            .fetch_one(self.pool.as_ref())
            .await?;
        Ok(exists)
    }
}
