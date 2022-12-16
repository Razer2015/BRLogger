use serde::Serialize;
use sqlx::{query_as, Transaction, MySql, query};

use super::context::BattlelogContext;

#[derive(Debug, Clone, Serialize)]
pub struct PersonaGameExpansion {
    pub persona_id: u64,
    pub game_expansion_id: u64,
    pub platforms: u32,
}

impl PersonaGameExpansion {
    pub fn new(persona_id: u64, game_expansion_id: u64, platforms: u32) -> Self {
        Self { 
            persona_id,
            game_expansion_id,
            platforms,
        }
    }
}

impl BattlelogContext {
    pub async fn get_persona_game_expansion_by_id(&self, persona_id: u64, game_expansion_id: u64) -> Result<Option<PersonaGameExpansion>, sqlx::Error> {
        let res =
            query_as!(PersonaGameExpansion, "SELECT * from persona_game_expansions WHERE persona_id = ? AND game_expansion_id = ?", persona_id, game_expansion_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(res)
    }

    pub async fn get_persona_game_expansions(&self, persona_id: u64) -> Result<Vec<PersonaGameExpansion>, sqlx::Error> {
        let res: Vec<PersonaGameExpansion> =
            query_as!(PersonaGameExpansion, "SELECT * from persona_game_expansions WHERE persona_id = ?", persona_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(res)
    }

    pub async fn insert_persona_game_expansion(&self, p: &PersonaGameExpansion) -> anyhow::Result<u64> {
        self.insert_persona_game_expansion_private(None, p).await
    }

    pub async fn insert_persona_game_expansion_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, p: &PersonaGameExpansion) -> anyhow::Result<u64> {
        self.insert_persona_game_expansion_private(Some(transaction), p).await
    }

    async fn insert_persona_game_expansion_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, p: &PersonaGameExpansion) -> anyhow::Result<u64> {
        let mut res = 0;

        let query = query!(r#"INSERT INTO persona_game_expansions (persona_id, game_expansion_id, platforms) VALUES (?, ?, ?)"#, p.persona_id, p.game_expansion_id, p.platforms);
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

    pub async fn upsert_persona_game_expansion(&self, transaction: &mut Transaction<'_, MySql>, p: &PersonaGameExpansion) -> anyhow::Result<u64> {
        let query = query!(r#"INSERT IGNORE INTO persona_game_expansions (persona_id, game_expansion_id, platforms) VALUES (?, ?, ?)"#, p.persona_id, p.game_expansion_id, p.platforms);
        let res = query
            .execute(&mut *transaction)
            .await?
            .last_insert_id();

        Ok(res)
    }
    
    pub async fn update_persona_game_expansion(&self, p: &PersonaGameExpansion) -> anyhow::Result<bool> {
        self.update_persona_game_expansion_private(None, p).await
    }

    pub async fn update_persona_game_expansion_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, p: &PersonaGameExpansion) -> anyhow::Result<bool> {
        self.update_persona_game_expansion_private(Some(transaction), p).await
    }

    async fn update_persona_game_expansion_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, p: &PersonaGameExpansion) -> anyhow::Result<bool> {
        let mut res: u64 = 0;

        let query = query!(r#"UPDATE persona_game_expansions SET platforms = ? WHERE persona_id = ? AND game_expansion_id = ?"#, p.platforms, p.persona_id, p.game_expansion_id);
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