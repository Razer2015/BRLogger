use serde::Serialize;
use sqlx::{query_as, query, MySql, Transaction};

use super::context::BattlelogContext;

#[derive(Debug, Clone, Serialize)]
pub struct BattlelogServer {
    pub id: i32,
    pub name: String,
    pub guid: String,
}

impl BattlelogServer {
    pub fn new(name: String, guid: String) -> Self {
        Self { 
            id: 0,
            name: name.to_string(),
            guid: guid.to_string(),
        }
    }

    pub fn new_with_id(id: i32, name: String, guid: String) -> Self {
        Self { 
            id: id,
            name: name,
            guid: guid,
        }
    }
}

impl BattlelogContext {
    pub async fn get_server_by_server_id(&self, server_id: i32) -> Result<Option<BattlelogServer>, sqlx::Error> {
        pub struct Row {
            pub id: i32,
            pub name: String,
            pub guid: String,
        }

        let res =
            query_as!(Row, "SELECT * from servers WHERE id = ?", server_id)
            .fetch_optional(&self.pool)
            .await?;

        let res = res.map(|e: Row| BattlelogServer {
            id: e.id,
            name: e.name,
            guid: e.guid,
        });

        Ok(res)
    }

    pub async fn get_server_by_server_guid(&self, guid: &str) -> Result<Option<BattlelogServer>, sqlx::Error> {
        pub struct Row {
            pub id: i32,
            pub name: String,
            pub guid: String,
        }

        let res =
            query_as!(Row, "SELECT * from servers WHERE guid = ?", guid)
            .fetch_optional(&self.pool)
            .await?;

        let res = res.map(|e: Row| BattlelogServer {
            id: e.id,
            name: e.name,
            guid: e.guid,
        });

        Ok(res)
    }

    pub async fn insert_server(&self, server: &BattlelogServer) -> anyhow::Result<u64> {
        self.insert_server_private(None, server).await
    }

    pub async fn insert_server_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, server: &BattlelogServer) -> anyhow::Result<u64> {
        self.insert_server_private(Some(transaction), server).await
    }

    async fn insert_server_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, server: &BattlelogServer) -> anyhow::Result<u64> {
        let mut res = 0;

        let query = query!(r#"INSERT INTO servers (name, guid) VALUES (?, ?)"#, server.name, server.guid);
        if transaction.is_some() { 
            res = query
                .execute(&mut *transaction.unwrap())
                .await?
                .last_insert_id();
        } 
        else { 
            res = query
                .execute(&self.pool)
                .await?
                .last_insert_id();
        }

        Ok(res)
    }
    
    pub async fn update_server(&self, server: &BattlelogServer) -> anyhow::Result<bool> {
        self.update_server_private(None, server).await
    }

    pub async fn update_server_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, server: &BattlelogServer) -> anyhow::Result<bool> {
        self.update_server_private(Some(transaction), server).await
    }

    async fn update_server_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, server: &BattlelogServer) -> anyhow::Result<bool> {
        let mut res: u64 = 0;

        let query = query!(r#"UPDATE servers SET name = ?, guid = ? WHERE id = ?"#, server.name, server.guid, server.id);
        if transaction.is_some() { 
            res = query
                .execute(&mut *transaction.unwrap())
                .await?
                .rows_affected();
        } 
        else { 
            res = query
                .execute(&self.pool)
                .await?
                .rows_affected();
        }

        Ok(res > 0)
    }
}