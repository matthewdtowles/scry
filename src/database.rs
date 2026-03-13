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



    pub async fn execute_raw(&self, query: &str) -> Result<()> {
        sqlx::raw_sql(query).execute(&*self.pool).await?;
        Ok(())
    }

    pub async fn row_exists(&self, table: &str, column: &str, value: &str) -> Result<bool> {
        let query = format!(
            r#"SELECT EXISTS(SELECT 1 FROM "{}" WHERE "{}" = $1)"#,
            table, column
        );
        let exists: bool = sqlx::query_scalar(&query)
            .bind(value)
            .fetch_one(self.pool.as_ref())
            .await?;
        Ok(exists)
    }
}
