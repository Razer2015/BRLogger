use std::{env, time::Duration, str::FromStr};

use sqlx::{MySqlPool, MySql, Error, Transaction, mysql::{MySqlPoolOptions, MySqlConnectOptions}, ConnectOptions};

#[derive(Clone, Debug)]
pub struct BattlelogContext {
    pub pool: MySqlPool,
}

impl BattlelogContext {
    pub fn new_from_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn connect(url: impl AsRef<str>) -> Result<Self, sqlx::Error> {
        let mut connection_options = MySqlConnectOptions::from_str(url.as_ref())?;
        connection_options
            .disable_statement_logging();
        let pool = MySqlPoolOptions::new().connect_with(connection_options).await?;
        // let pool = MySqlPool::connect(url.as_ref()).await?; // TODO: unwrap
        Ok(Self { pool })
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Create a new `BattlelogContext` using a connection string form the `DATABASE_URL` environment
    /// variable, with something like `mysql://username:password@host/database`.
    /// You may need to initialize `dotenv` yourself if you haven't done so yet.
    ///
    /// A connection will only be made when necessary.
    pub fn new_env() -> Self {
        let url = env::var("DATABASE_URL").unwrap(); // TODO: unwrap
        // lazy: will only connect when needed.
        let pool = MySqlPool::connect_lazy(&url).unwrap(); // TODO: unwrap
        Self {
            pool
        }
    }

    pub async fn begin_transaction(&self) -> Result<Transaction<'_, MySql>, Error> {
        self.pool.begin().await
    }
}

#[cfg(test)]
mod test {
    use anyhow::Context;

    use crate::database::battlelog::{servers::BattlelogServer, personas::BattlelogPersona, battlereports::BattlelogBattlereport, playerreports::BattlelogPlayerreport};

    use super::BattlelogContext;

    fn get_db_coninfo() -> anyhow::Result<String> {
        dotenv::dotenv()?;
        let uri = std::env::var("DATABASE_URL")
            .context("Need to specify Battlelog db URI via env var, for example DATABASE_URL=\"mysql://username:password@host/database\"")?;
        Ok(uri)
    }

    #[ignore]
    #[tokio::test]
    async fn test_server_insert() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let server_id = db.insert_server(&BattlelogServer::new("".to_string(), "4d0151b3-81ff-4268-b4e8-5e60d5bc8765".to_string())).await?;
        println!("{server_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_server_update() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let server_id = db.update_server(&BattlelogServer::new_with_id(2, "LSD".to_string(), "4d0151b3-81ff-4268-b4e8-5e60d5bc8765".to_string())).await?;
        println!("{server_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_persona_insert() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let persona_id = db.insert_persona(&BattlelogPersona::new(824078704, Some("Tatarek99".to_string()), None, None, false)).await?;
        println!("{persona_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_persona_update() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let server_id = db.update_persona(&BattlelogPersona::new(824078704, Some("Tatarek99".to_string()), Some("PLT".to_string()), None, false)).await?;
        println!("{server_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_battlereport_insert() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let persona_id = db.insert_battlereport(&BattlelogBattlereport::new(1297613665940962880, 1261, 1, 1, "".to_string(), "2".to_string(), 1598211430, 0)).await?;
        println!("{persona_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_battlereport_update() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let server_id = db.update_battlereport(&BattlelogBattlereport::new(1297613665940962880, 1261, 1, 1, "MP_Tremors".to_string(), "2".to_string(), 1598211430, 1)).await?;
        println!("{server_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_playerreport_insert() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let persona_id = db.insert_playerreport(&BattlelogPlayerreport::new(1297613665940962880, 824078704, 
            0, 1, 1.0, 29.0, 0, 0, 0, 0.0, 0, 0, 3,
            0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0,
            0, 0, 1, 0, 0, 0.0344827586, false, false, true)).await?;
        println!("{persona_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_playerreport_update() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let server_id = db.update_playerreport(&BattlelogPlayerreport::new(1297613665940962880, 824078704, 
            0, 1, 1.0, 29.0, 0, 0, 0, 0.0, 0, 0, 3,
            0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0,
            0, 0, 1, 0, 0, 0.0344827586, true, false, true)).await?;
        println!("{server_id:#?}");
        panic!()
    }

    #[ignore]
    #[tokio::test]
    async fn test_server_insert_rollback() -> anyhow::Result<()> {
        let uri = get_db_coninfo()?;
        let db = BattlelogContext::connect(uri).await?;
        let mut transaction = db.begin_transaction().await?;
        let server_id = db.insert_server_with_transaction(&mut transaction, &BattlelogServer::new("Test".to_string(), "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".to_string())).await?;
        println!("{server_id:#?}");
        transaction.rollback().await?;
        panic!()
    }
}