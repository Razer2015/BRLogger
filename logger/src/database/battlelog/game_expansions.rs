use serde::Serialize;
use sqlx::{query_as};

use super::context::BattlelogContext;

#[derive(Debug, Clone, Serialize)]
pub struct GameExpansion {
    pub id: u64,
    pub value: Option<String>,
    pub explanation: Option<String>,
}

impl BattlelogContext {
    pub async fn get_game_expansion_by_id(&self, id: u64) -> Result<Option<GameExpansion>, sqlx::Error> {
        let res =
            query_as!(GameExpansion, "SELECT * from game_expansions WHERE id = ?", id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(res)
    }

    pub async fn get_game_expansions(&self) -> Result<Vec<GameExpansion>, sqlx::Error> {
        let res =
            query_as!(GameExpansion, "SELECT * from game_expansions")
            .fetch_all(&self.pool)
            .await?;

        Ok(res)
    }
}