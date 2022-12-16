use serde::Serialize;
use sqlx::{query_as, Transaction, MySql, query};

use super::context::BattlelogContext;

#[derive(Debug, Clone, Serialize)]
pub struct PersonaInfo {
    pub persona_id: u64,
    pub locality: Option<String>,
    pub location: Option<String>,
    pub presentation: Option<String>,
    pub login_counter: Option<u32>,
    pub last_login: Option<u32>,
}

impl PersonaInfo {
    pub fn new(persona_id: u64, locality: Option<String>, location: Option<String>, presentation: Option<String>, login_counter: Option<u32>, last_login: Option<u32>) -> Self {
        Self { 
            persona_id,
            locality,
            location,
            presentation,
            login_counter,
            last_login,
        }
    }
}

impl BattlelogContext {
    pub async fn get_persona_info_by_id(&self, persona_id: u64) -> Result<Option<PersonaInfo>, sqlx::Error> {
        let res =
            query_as!(PersonaInfo, "SELECT * from persona_infos WHERE persona_id = ?", persona_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(res)
    }

    pub async fn get_persona_infos(&self) -> Result<Vec<PersonaInfo>, sqlx::Error> {
        let mut res: Vec<PersonaInfo> =
            query_as!(PersonaInfo, "SELECT * from persona_infos")
            .fetch_all(&self.pool)
            .await?;

        Ok(res)
    }

    pub async fn insert_persona_info(&self, p: &PersonaInfo) -> anyhow::Result<u64> {
        self.insert_persona_info_private(None, p).await
    }

    pub async fn insert_persona_info_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, p: &PersonaInfo) -> anyhow::Result<u64> {
        self.insert_persona_info_private(Some(transaction), p).await
    }

    async fn insert_persona_info_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, p: &PersonaInfo) -> anyhow::Result<u64> {
        let mut res = 0;

        let query = query!(r#"INSERT INTO persona_infos (persona_id, locality, location, presentation, login_counter, last_login) VALUES (?, ?, ?, ?, ?, ?)"#, p.persona_id, p.locality, p.location, p.presentation, p.login_counter, p.last_login);
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

    pub async fn upsert_persona_info(&self, transaction: &mut Transaction<'_, MySql>, p: &PersonaInfo) -> anyhow::Result<u64> {
        let query = query!(r#"INSERT IGNORE INTO persona_infos (persona_id, locality, location, presentation, login_counter, last_login) VALUES (?, ?, ?, ?, ?, ?)"#, p.persona_id, p.locality, p.location, p.presentation, p.login_counter, p.last_login);
        let res = query
            .execute(&mut *transaction)
            .await?
            .last_insert_id();

        Ok(res)
    }
    
    pub async fn update_persona_info(&self, p: &PersonaInfo) -> anyhow::Result<bool> {
        self.update_persona_info_private(None, p).await
    }

    pub async fn update_persona_info_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, p: &PersonaInfo) -> anyhow::Result<bool> {
        self.update_persona_info_private(Some(transaction), p).await
    }

    async fn update_persona_info_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, p: &PersonaInfo) -> anyhow::Result<bool> {
        let mut res: u64 = 0;

        let query = query!(r#"UPDATE persona_infos SET locality = ?, location = ?, presentation = ?, login_counter = ?, last_login = ? WHERE persona_id = ?"#, p.locality, p.location, p.presentation, p.login_counter, p.last_login, p.persona_id);
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