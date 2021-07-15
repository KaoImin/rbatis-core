#![allow(unreachable_patterns)]

//other import
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use sqlx_core::acquire::Acquire;
use sqlx_core::arguments::{Arguments, IntoArguments};
use sqlx_core::connection::{Connection, ConnectOptions};
use sqlx_core::database::Database;
use sqlx_core::encode::Encode;
use sqlx_core::executor::Executor;
#[cfg(feature = "mssql")]
use sqlx_core::mssql::{
    Mssql, MssqlArguments, MssqlConnection, MssqlConnectOptions, MssqlPool, MssqlQueryResult, MssqlRow,
};
#[cfg(feature = "mysql")]
use sqlx_core::mysql::{
    MySql, MySqlArguments, MySqlConnection, MySqlConnectOptions, MySqlPool, MySqlQueryResult, MySqlRow,
    MySqlSslMode,
};
use sqlx_core::pool::{PoolConnection, Pool};
#[cfg(feature = "postgres")]
use sqlx_core::postgres::{
    PgArguments, PgConnection, PgConnectOptions, PgPool, PgPoolOptions, PgQueryResult, PgRow, PgSslMode,
    Postgres,
};
use sqlx_core::query::{query, Query};
#[cfg(feature = "sqlite")]
use sqlx_core::sqlite::{
    Sqlite, SqliteArguments, SqliteConnection, SqliteConnectOptions, SqlitePool, SqliteQueryResult,
    SqliteRow,
};
use sqlx_core::transaction::Transaction;
use sqlx_core::types::Type;

use crate::convert::{RefJsonCodec, ResultCodec};
use crate::db::{DBPoolOptions, DriverType};
use crate::decode::json_decode;
use crate::Error;
use crate::Result;
use crate::runtime::sync::Mutex;
use std::ops::DerefMut;

#[derive(Debug, Clone)]
pub struct DBPool {
    pub driver_type: DriverType,
    #[cfg(feature = "mysql")]
    pub mysql: Option<MySqlPool>,
    #[cfg(feature = "postgres")]
    pub postgres: Option<PgPool>,
    #[cfg(feature = "sqlite")]
    pub sqlite: Option<SqlitePool>,
    #[cfg(feature = "mssql")]
    pub mssql: Option<MssqlPool>,
}

impl DBPool {
    //new with default opt
    pub async fn new(driver: &str) -> crate::Result<DBPool> {
        return Self::new_opt_str(driver, &DBPoolOptions::default()).await;
    }

    //new with str
    pub async fn new_opt_str(driver: &str, opt: &DBPoolOptions) -> crate::Result<DBPool> {
        let conn_opt = DBConnectOption::from(driver)?;
        return Self::new_opt(&conn_opt, opt).await;
    }

    //new_opt from DBConnectionOption option and PoolOptions
    pub async fn new_opt(driver: &DBConnectOption, opt: &DBPoolOptions) -> crate::Result<DBPool> {
        let mut pool = Self {
            driver_type: DriverType::None,
            #[cfg(feature = "mysql")]
            mysql: None,
            #[cfg(feature = "postgres")]
            postgres: None,
            #[cfg(feature = "sqlite")]
            sqlite: None,
            #[cfg(feature = "mssql")]
            mssql: None,
        };
        match &driver.driver_type {
            #[cfg(feature = "mysql")]
            DriverType::Mysql => {
                pool.driver_type = DriverType::Mysql;
                let build = sqlx_core::pool::PoolOptions::<MySql>::default()
                    .max_connections(opt.max_connections)
                    .max_lifetime(opt.max_lifetime)
                    .connect_timeout(opt.connect_timeout)
                    .min_connections(opt.min_connections)
                    .idle_timeout(opt.idle_timeout)
                    .test_before_acquire(opt.test_before_acquire);
                let p = build.connect_with(driver.mysql.clone().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?).await?;
                pool.mysql = Some(p);
                return Ok(pool);
            }
            #[cfg(feature = "postgres")]
            DriverType::Postgres => {
                pool.driver_type = DriverType::Postgres;
                let build = sqlx_core::pool::PoolOptions::<Postgres>::new()
                    .max_connections(opt.max_connections)
                    .max_lifetime(opt.max_lifetime)
                    .connect_timeout(opt.connect_timeout)
                    .min_connections(opt.min_connections)
                    .idle_timeout(opt.idle_timeout)
                    .test_before_acquire(opt.test_before_acquire);
                let p = build.connect_with(driver.postgres.clone().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?).await?;
                pool.postgres = Some(p);
                return Ok(pool);
            }
            #[cfg(feature = "sqlite")]
            DriverType::Sqlite => {
                pool.driver_type = DriverType::Sqlite;
                let build = sqlx_core::pool::PoolOptions::<Sqlite>::new()
                    .max_connections(opt.max_connections)
                    .max_lifetime(opt.max_lifetime)
                    .connect_timeout(opt.connect_timeout)
                    .min_connections(opt.min_connections)
                    .idle_timeout(opt.idle_timeout)
                    .test_before_acquire(opt.test_before_acquire);
                let p = build.connect_with(driver.sqlite.clone().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?).await?;
                pool.sqlite = Some(p);
                return Ok(pool);
            }
            #[cfg(feature = "mssql")]
            DriverType::Mssql => {
                pool.driver_type = DriverType::Mssql;
                let build = sqlx_core::pool::PoolOptions::<Mssql>::new()
                    .max_connections(opt.max_connections)
                    .max_lifetime(opt.max_lifetime)
                    .connect_timeout(opt.connect_timeout)
                    .min_connections(opt.min_connections)
                    .idle_timeout(opt.idle_timeout)
                    .test_before_acquire(opt.test_before_acquire);
                let p = build.connect_with(driver.mssql.clone().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?).await?;
                pool.mssql = Some(p);
                return Ok(pool);
            }
            _ => {
                return Err(Error::from(
                    "unsupport driver type or not enable target database feature!",
                ));
            }
        }
    }

    pub fn make_db_query<'f, 's>(driver_type: &DriverType, sql: &'s str) -> crate::Result<DBQuery<'s>> {
        match driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            &DriverType::Mysql => {
                return Ok(DBQuery {
                    driver_type: DriverType::Mysql,
                    #[cfg(feature = "mysql")]
                    mysql: Some(query(sql)),
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: None,
                });
            }
            &DriverType::Postgres => {
                return Ok(DBQuery {
                    driver_type: DriverType::Postgres,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: Some(query(sql)),
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: None,
                });
            }
            &DriverType::Sqlite => {
                return Ok(DBQuery {
                    driver_type: DriverType::Sqlite,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: Some(query(sql)),
                    #[cfg(feature = "mssql")]
                    mssql: None,
                });
            }
            &DriverType::Mssql => {
                return Ok(DBQuery {
                    driver_type: DriverType::Mssql,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: Some(query(sql)),
                });
            }
        }
    }

    pub fn make_query<'f, 's>(&'f self, sql: &'s str) -> crate::Result<DBQuery<'s>> {
        return Self::make_db_query(&self.driver_type, sql);
    }
    /// Retrieves a connection from the pool.
    ///
    /// Waits for at most the configured connection timeout before returning an error.
    pub async fn acquire(&self) -> crate::Result<DBPoolConn> {
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                return Ok(DBPoolConn {
                    driver_type: DriverType::Mysql,
                    #[cfg(feature = "mysql")]
                    mysql: Some(self.mysql.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.acquire().await?),
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: None,
                });
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                return Ok(DBPoolConn {
                    driver_type: DriverType::Postgres,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: Some(self.postgres.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.acquire().await?),
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: None,
                });
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                return Ok(DBPoolConn {
                    driver_type: DriverType::Sqlite,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: Some(self.sqlite.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.acquire().await?),
                    #[cfg(feature = "mssql")]
                    mssql: None,
                });
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                return Ok(DBPoolConn {
                    driver_type: DriverType::Mssql,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: Some(self.mssql.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.acquire().await?),
                });
            }

            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }

    /// Attempts to retrieve a connection from the pool if there is one available.
    ///
    /// Returns `None` immediately if there are no idle connections available in the pool.
    pub fn try_acquire(&self) -> crate::Result<Option<DBPoolConn>> {
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                let conn = self.mysql.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.try_acquire();
                if conn.is_none() {
                    return Ok(None);
                }
                return Ok(Some(DBPoolConn {
                    driver_type: self.driver_type,
                    #[cfg(feature = "mysql")]
                    mysql: Some(conn.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?),
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: None,
                }));
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                let conn = self.postgres.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.try_acquire();
                if conn.is_none() {
                    return Ok(None);
                }
                return Ok(Some(DBPoolConn {
                    driver_type: self.driver_type,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: Some(conn.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?),
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: None,
                }));
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                let conn = self.sqlite.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.try_acquire();
                if conn.is_none() {
                    return Ok(None);
                }
                return Ok(Some(DBPoolConn {
                    driver_type: self.driver_type,
                    #[cfg(feature = "mysql")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: Some(conn.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?),
                    #[cfg(feature = "mssql")]
                    mssql: None,
                }));
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                let conn = self.mssql.as_ref().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.try_acquire();
                if conn.is_none() {
                    return Ok(None);
                }
                return Ok(Some(DBPoolConn {
                    driver_type: self.driver_type,
                    #[cfg(feature = "postgres")]
                    mysql: None,
                    #[cfg(feature = "postgres")]
                    postgres: None,
                    #[cfg(feature = "sqlite")]
                    sqlite: None,
                    #[cfg(feature = "mssql")]
                    mssql: Some(conn.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?),
                }));
            }

            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }

    pub async fn begin(&self) -> crate::Result<DBTx> {
        let mut tx = DBTx {
            driver_type: self.driver_type.clone(),
            conn: Some(self.acquire().await?),
            done: true,
        };
        tx.begin().await?;
        Ok(tx)
    }

    pub async fn close(&self) {
        match &self.driver_type {
            &DriverType::None => {
                return;
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                match &self.mysql {
                    None => {}
                    Some(pool) => {
                        pool.close().await;
                    }
                }
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                match &self.postgres {
                    None => {}
                    Some(pool) => {
                        pool.close().await;
                    }
                }
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                match &self.sqlite {
                    None => {}
                    Some(pool) => {
                        pool.close().await;
                    }
                }
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                match &self.mssql {
                    None => {}
                    Some(pool) => {
                        pool.close().await;
                    }
                }
            }
            _ => {
                return;
            }
        }
    }
}

/// DBConnectOption all of support Database Options abstract struct.
/// use from(url:&str) or use from_mysql(),from_pg().... or other method init this.
#[derive(Debug, Clone)]
pub struct DBConnectOption {
    pub driver_type: DriverType,
    #[cfg(feature = "mysql")]
    pub mysql: Option<MySqlConnectOptions>,
    #[cfg(feature = "postgres")]
    pub postgres: Option<PgConnectOptions>,
    #[cfg(feature = "sqlite")]
    pub sqlite: Option<SqliteConnectOptions>,
    #[cfg(feature = "mssql")]
    pub mssql: Option<MssqlConnectOptions>,
}

impl DBConnectOption {
    #[cfg(feature = "mysql")]
    pub fn from_mysql(conn_opt: &MySqlConnectOptions) -> Result<Self> {
        let mut conn_opt = conn_opt.clone();
        conn_opt.log_slow_statements(log::LevelFilter::Off, Duration::from_secs(0));
        conn_opt.log_statements(log::LevelFilter::Off);
        return Ok(DBConnectOption {
            driver_type: DriverType::Mysql,
            #[cfg(feature = "mysql")]
            mysql: Some(conn_opt),
            #[cfg(feature = "postgres")]
            postgres: None,
            #[cfg(feature = "sqlite")]
            sqlite: None,
            #[cfg(feature = "mssql")]
            mssql: None,
        });
    }
    #[cfg(feature = "postgres")]
    pub fn from_pg(conn_opt: &PgConnectOptions) -> Result<Self> {
        let mut conn_opt = conn_opt.clone();
        conn_opt.log_slow_statements(log::LevelFilter::Off, Duration::from_secs(0));
        conn_opt.log_statements(log::LevelFilter::Off);
        return Ok(Self {
            driver_type: DriverType::Postgres,
            #[cfg(feature = "mysql")]
            mysql: None,
            #[cfg(feature = "postgres")]
            postgres: Some(conn_opt),
            #[cfg(feature = "sqlite")]
            sqlite: None,
            #[cfg(feature = "mssql")]
            mssql: None,
        });
    }

    #[cfg(feature = "sqlite")]
    pub fn from_sqlite(conn_opt: &SqliteConnectOptions) -> Result<Self> {
        let mut conn_opt = conn_opt.clone();
        conn_opt.log_slow_statements(log::LevelFilter::Off, Duration::from_secs(0));
        conn_opt.log_statements(log::LevelFilter::Off);
        return Ok(Self {
            driver_type: DriverType::Sqlite,
            #[cfg(feature = "mysql")]
            mysql: None,
            #[cfg(feature = "postgres")]
            postgres: None,
            #[cfg(feature = "sqlite")]
            sqlite: Some(conn_opt),
            #[cfg(feature = "mssql")]
            mssql: None,
        });
    }

    #[cfg(feature = "mssql")]
    pub fn from_mssql(conn_opt: &MssqlConnectOptions) -> Result<Self> {
        let mut conn_opt = conn_opt.clone();
        conn_opt.log_slow_statements(log::LevelFilter::Off, Duration::from_secs(0));
        conn_opt.log_statements(log::LevelFilter::Off);
        return Ok(Self {
            driver_type: DriverType::Mssql,
            #[cfg(feature = "mysql")]
            mysql: None,
            #[cfg(feature = "postgres")]
            postgres: None,
            #[cfg(feature = "sqlite")]
            sqlite: None,
            #[cfg(feature = "mssql")]
            mssql: Some(conn_opt),
        });
    }

    pub fn from(driver: &str) -> Result<Self> {
        if driver.starts_with("mysql") {
            #[cfg(feature = "mysql")]
                {
                    let mut conn_opt = MySqlConnectOptions::from_str(driver)?;
                    if !driver.contains("ssl-mode") {
                        conn_opt = conn_opt.ssl_mode(MySqlSslMode::Disabled);
                    }
                    return Self::from_mysql(&conn_opt);
                }
            #[cfg(not(feature = "mysql"))]
                {
                    return Err(Error::from("[rbatis] not enable feature!"));
                }
        } else if driver.starts_with("postgres") {
            #[cfg(feature = "postgres")]
                {
                    let mut conn_opt = PgConnectOptions::from_str(driver)?;
                    if !driver.contains("ssl-mode") && !driver.contains("sslmode") {
                        conn_opt = conn_opt.ssl_mode(PgSslMode::Disable);
                    }
                    return Self::from_pg(&conn_opt);
                }
            #[cfg(not(feature = "postgres"))]
                {
                    return Err(Error::from("[rbatis] not enable feature!"));
                }
        } else if driver.starts_with("sqlite") {
            #[cfg(feature = "sqlite")]
                {
                    let conn_opt = SqliteConnectOptions::from_str(driver)?;
                    return Self::from_sqlite(&conn_opt);
                }
            #[cfg(not(feature = "sqlite"))]
                {
                    return Err(Error::from("[rbatis] not enable feature!"));
                }
        } else if driver.starts_with("mssql") || driver.starts_with("sqlserver") {
            #[cfg(feature = "mssql")]
                {
                    let conn_opt = MssqlConnectOptions::from_str(driver)?;
                    return Self::from_mssql(&conn_opt);
                }
            #[cfg(not(feature = "mssql"))]
                {
                    return Err(Error::from("[rbatis] not enable feature!"));
                }
        } else {
            return Err(Error::from("unsupport driver type!"));
        }
    }
}


pub struct DBQuery<'q> {
    pub driver_type: DriverType,
    #[cfg(feature = "mysql")]
    pub mysql: Option<Query<'q, MySql, MySqlArguments>>,
    #[cfg(feature = "postgres")]
    pub postgres: Option<Query<'q, Postgres, PgArguments>>,
    #[cfg(feature = "sqlite")]
    pub sqlite: Option<Query<'q, Sqlite, SqliteArguments<'q>>>,
    #[cfg(feature = "mssql")]
    pub mssql: Option<Query<'q, Mssql, MssqlArguments>>,
}


impl<'q> DBQuery<'q> {
    pub fn bind_value(&mut self, t: &serde_json::Value) -> crate::Result<()> {
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                let mut q = self.mysql.take().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?;
                match t {
                    serde_json::Value::String(s) => {
                        q = q.bind(Some(s.to_owned()));
                    }
                    serde_json::Value::Null => {
                        q = q.bind(Option::<i32>::None);
                    }
                    serde_json::Value::Number(n) => {
                        if n.is_f64() {
                            q = q.bind(n.as_f64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_u64() {
                            q = q.bind(n.as_u64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_i64() {
                            q = q.bind(n.as_i64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        q = q.bind(Option::Some(b.to_owned()));
                    }
                    _ => {
                        q = q.bind(Some(t.to_string()));
                    }
                }
                self.mysql = Some(q);
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                let mut q = self.postgres.take().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?;
                match t {
                    serde_json::Value::String(s) => {
                        q = q.bind(Some(s.to_owned()));
                    }
                    serde_json::Value::Null => {
                        q = q.bind(Option::<i32>::None);
                    }
                    serde_json::Value::Number(n) => {
                        if n.is_f64() {
                            q = q.bind(n.as_f64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_i64() {
                            q = q.bind(n.as_i64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_u64() {
                            q = q.bind(n.as_i64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        q = q.bind(Option::Some(b.to_owned()));
                    }
                    _ => {
                        q = q.bind(Some(t.to_string()));
                    }
                }
                self.postgres = Some(q);
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                let mut q = self.sqlite.take().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?;
                match t {
                    serde_json::Value::String(s) => {
                        q = q.bind(Some(s.to_owned()));
                    }
                    serde_json::Value::Null => {
                        q = q.bind(Option::<i32>::None);
                    }
                    serde_json::Value::Number(n) => {
                        if n.is_f64() {
                            q = q.bind(n.as_f64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_i64() {
                            q = q.bind(n.as_i64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_u64() {
                            q = q.bind(n.as_i64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        q = q.bind(Option::Some(b.to_owned()));
                    }
                    _ => {
                        q = q.bind(Some(t.to_string()));
                    }
                }
                self.sqlite = Some(q);
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                let mut q = self.mssql.take().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?;
                match t {
                    serde_json::Value::String(s) => {
                        q = q.bind(Some(s.to_owned()));
                    }
                    serde_json::Value::Null => {
                        q = q.bind(Option::<i32>::None);
                    }
                    serde_json::Value::Number(n) => {
                        if n.is_f64() {
                            q = q.bind(n.as_f64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_i64() {
                            q = q.bind(n.as_i64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        } else if n.is_u64() {
                            q = q.bind(n.as_i64().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        q = q.bind(Option::Some(b.to_owned()));
                    }
                    _ => {
                        q = q.bind(Some(t.to_string()));
                    }
                }
                self.mssql = Some(q);
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
        return Ok(());
    }
}

#[derive(Debug)]
pub struct DBPoolConn {
    pub driver_type: DriverType,
    #[cfg(feature = "mysql")]
    pub mysql: Option<PoolConnection<MySql>>,
    #[cfg(feature = "postgres")]
    pub postgres: Option<PoolConnection<Postgres>>,
    #[cfg(feature = "sqlite")]
    pub sqlite: Option<PoolConnection<Sqlite>>,
    #[cfg(feature = "mssql")]
    pub mssql: Option<PoolConnection<Mssql>>,
}

impl DBPoolConn {
    pub fn check_alive(&self) -> crate::Result<()> {
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                if self.mysql.is_none() {
                    return Err(Error::from("un init DBPoolConn!"));
                }
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                if self.postgres.is_none() {
                    return Err(Error::from("un init DBPoolConn!"));
                }
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                if self.sqlite.is_none() {
                    return Err(Error::from("un init DBPoolConn!"));
                }
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                if self.mssql.is_none() {
                    return Err(Error::from("un init DBPoolConn!"));
                }
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }

        return Ok(());
    }

    pub async fn fetch<'q, T>(&mut self, sql: &'q str) -> crate::Result<(T, usize)>
        where
            T: DeserializeOwned,
    {
        self.check_alive()?;
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                let async_stream: Vec<MySqlRow> =
                    self.mysql.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.fetch_all(sql).await?;
                let json_array = async_stream.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                let async_stream: Vec<PgRow> =
                    self.postgres.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.fetch_all(sql).await?;
                let json_array = async_stream.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                let data: Vec<SqliteRow> = self.sqlite.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.fetch_all(sql).await?;
                let json_array = data.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                let async_stream: Vec<MssqlRow> =
                    self.mssql.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.fetch_all(sql).await?;
                let json_array = async_stream.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }

    pub async fn exec(&mut self, sql: &str) -> crate::Result<DBExecResult> {
        self.check_alive()?;
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                let data: MySqlQueryResult = self.mysql.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.execute(sql).await?;
                return Ok(DBExecResult::from(data));
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                let data: PgQueryResult = self.postgres.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.execute(sql).await?;
                return Ok(DBExecResult::from(data));
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                let data: SqliteQueryResult = self.sqlite.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.execute(sql).await?;
                return Ok(DBExecResult::from(data));
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                let data: MssqlQueryResult = self.mssql.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.execute(sql).await?;
                return Ok(DBExecResult::from(data));
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }

    pub async fn fetch_parperd<T>(&mut self, sql: DBQuery<'_>) -> crate::Result<(T, usize)>
        where
            T: DeserializeOwned,
    {
        self.check_alive()?;
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                let data: Vec<MySqlRow> = self
                    .mysql
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .fetch_all(sql.mysql.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                let json_array = data.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                let data: Vec<PgRow> = self
                    .postgres
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .fetch_all(sql.postgres.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                let json_array = data.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                let data: Vec<SqliteRow> = self
                    .sqlite
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .fetch_all(sql.sqlite.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                let json_array = data.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                let data: Vec<MssqlRow> = self
                    .mssql
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .fetch_all(sql.mssql.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                let json_array = data.try_to_json()?.as_array().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.to_owned();
                let return_len = json_array.len();
                let result = json_decode::<T>(json_array)?;
                Ok((result, return_len))
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }

    pub async fn exec_prepare(&mut self, sql: DBQuery<'_>) -> crate::Result<DBExecResult> {
        self.check_alive()?;
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                let result: MySqlQueryResult = self
                    .mysql
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .execute(sql.mysql.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                return Ok(DBExecResult::from(result));
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                let data: PgQueryResult = self
                    .postgres
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .execute(sql.postgres.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                return Ok(DBExecResult::from(data));
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                let data: SqliteQueryResult = self
                    .sqlite
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .execute(sql.sqlite.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                return Ok(DBExecResult::from(data));
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                let data: MssqlQueryResult = self
                    .mssql
                    .as_mut()
                    .ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?
                    .execute(sql.mssql.ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?)
                    .await?;
                return Ok(DBExecResult::from(data));
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }

    pub async fn begin(mut self) -> crate::Result<DBTx> {
        self.check_alive()?;
        let mut tx = DBTx {
            driver_type: self.driver_type.clone(),
            conn: Some(self),
            done: true,
        };
        tx.begin().await;
        return Ok(tx);
    }

    pub async fn ping(&mut self) -> crate::Result<()> {
        self.check_alive()?;
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                return Ok(self.mysql.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.ping().await?);
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                return Ok(self.postgres.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.ping().await?);
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                return Ok(self.sqlite.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.ping().await?);
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                return Ok(self.mssql.as_mut().ok_or_else(|| Error::from("[rbatis-core]  conn is none!"))?.ping().await?);
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }

    pub async fn close(mut self) -> crate::Result<()> {
        match &self.driver_type {
            &DriverType::None => {
                return Err(Error::from("un init DBPool!"));
            }
            #[cfg(feature = "mysql")]
            &DriverType::Mysql => {
                self.mysql = None;
                return Ok(());
            }
            #[cfg(feature = "postgres")]
            &DriverType::Postgres => {
                self.postgres = None;
                return Ok(());
            }
            #[cfg(feature = "sqlite")]
            &DriverType::Sqlite => {
                self.sqlite = None;
                return Ok(());
            }
            #[cfg(feature = "mssql")]
            &DriverType::Mssql => {
                self.mssql = None;
                return Ok(());
            }
            _ => {
                return Err(Error::from("[rbatis] feature not enable!"));
            }
        }
    }
}

#[derive(Debug)]
pub struct DBTx {
    pub driver_type: DriverType,
    pub conn: Option<DBPoolConn>,
    /// is tx done?
    pub done: bool,
}

impl DBTx {
    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn take(mut self) -> Option<DBPoolConn> {
        self.conn
    }

    pub async fn begin(&mut self) -> crate::Result<()> {
        if !self.done {
            return Ok(());
        }
        let conn = self.conn.as_mut().ok_or_else(|| Error::from("[rbatis-core] DBTx conn is none!"))?;
        conn.exec("BEGIN").await?;
        self.done = false;
        return Ok(());
    }

    pub async fn commit(&mut self) -> crate::Result<()> {
        let conn = self.conn.as_mut().ok_or_else(|| Error::from("[rbatis-core] DBTx conn is none!"))?;
        conn.exec("COMMIT").await?;
        self.done = true;
        return Ok(());
    }

    pub async fn rollback(&mut self) -> crate::Result<()> {
        let conn = self.conn.as_mut().ok_or_else(|| Error::from("[rbatis-core] DBTx conn is none!"))?;
        conn.exec("ROLLBACK").await?;
        self.done = true;
        return Ok(());
    }

    pub async fn fetch<'q, T>(&mut self, sql: &'q str) -> crate::Result<(T, usize)>
        where
            T: DeserializeOwned,
    {
        let conn = self.conn.as_mut().ok_or_else(|| Error::from("[rbatis-core] DBTx conn is none!"))?;
        return conn.fetch(sql).await;
    }

    pub async fn fetch_parperd<'q, T>(&mut self, sql: DBQuery<'q>) -> crate::Result<(T, usize)>
        where
            T: DeserializeOwned,
    {
        let conn = self.conn.as_mut().ok_or_else(|| Error::from("[rbatis-core] DBTx conn is none!"))?;
        return conn.fetch_parperd(sql).await;
    }

    pub async fn exec(&mut self, sql: &str) -> crate::Result<DBExecResult> {
        let conn = self.conn.as_mut().ok_or_else(|| Error::from("[rbatis-core] DBTx conn is none!"))?;
        return conn.exec(sql).await;
    }

    pub async fn exec_prepare(&mut self, sql: DBQuery<'_>) -> crate::Result<DBExecResult> {
        let conn = self.conn.as_mut().ok_or_else(|| Error::from("[rbatis-core] DBTx conn is none!"))?;
        return conn.exec_prepare(sql).await;
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBExecResult {
    pub rows_affected: u64,
    pub last_insert_id: Option<i64>,
}

#[cfg(feature = "mysql")]
impl From<MySqlQueryResult> for DBExecResult {
    fn from(arg: MySqlQueryResult) -> Self {
        Self {
            rows_affected: arg.rows_affected(),
            last_insert_id: Some(arg.last_insert_id() as i64),
        }
    }
}

#[cfg(feature = "postgres")]
impl From<PgQueryResult> for DBExecResult {
    fn from(arg: PgQueryResult) -> Self {
        Self {
            rows_affected: arg.rows_affected(),
            last_insert_id: None,
        }
    }
}

#[cfg(feature = "sqlite")]
impl From<SqliteQueryResult> for DBExecResult {
    fn from(arg: SqliteQueryResult) -> Self {
        Self {
            rows_affected: arg.rows_affected(),
            last_insert_id: Some(arg.last_insert_rowid()),
        }
    }
}

#[cfg(feature = "mssql")]
impl From<MssqlQueryResult> for DBExecResult {
    fn from(arg: MssqlQueryResult) -> Self {
        Self {
            rows_affected: arg.rows_affected(),
            last_insert_id: None,
        }
    }
}
