use battlelog::BattlereportResponse;
use serde::Serialize;
use sqlx::{query_as, query, MySql, Transaction};

use super::context::BattlelogContext;

#[derive(Debug, Serialize)]
pub struct BattlelogBattlereport {
    pub id: u64,
    pub duration: u32,
    pub winner: i8,
    pub server_id: i32,
    pub map: String,
    pub mode: String,
    pub created_at: u32,
    pub processed: u8,
}

impl BattlelogBattlereport {
    pub fn new(id: u64, duration: u32, winner: i8, server_id: i32, map: String, mode: String, created_at: u32, processed: u8) -> Self {
        Self { 
            id,
            duration,
            winner,
            server_id,
            map,
            mode,
            created_at,
            processed,
        }
    }

    pub fn from_battlereport_response(report: &BattlereportResponse, server_id: i32) -> Self {
        Self { 
            id: report.id.parse::<u64>().unwrap(),
            duration: report.duration as u32,
            winner: BattlelogBattlereport::get_winner(&report),
            server_id,
            map: report.game_server.map.as_ref().unwrap().to_string(),
            mode: report.game_server.map_mode.as_ref().unwrap().to_string(),
            created_at: report.created_at,
            processed: 0,
        }
    }

    // TODO: This should be implemented in the BattlereportResponse
    fn get_winner(battlereport: &BattlereportResponse) -> i8 {
        for team in battlereport.teams.iter() {
            let team = team.1;
            if team.is_winner {
                return team.id;
            }
        }
    
        -1
    }
}

impl BattlelogContext {
    pub async fn get_battlereport_by_report_id(&self, report_id: u64) -> Result<Option<BattlelogBattlereport>, sqlx::Error> {
        pub struct Row {
            pub id: u64,
            pub duration: u32,
            pub winner: i8,
            pub server_id: i32,
            pub map: String,
            pub mode: String,
            pub created_at: u32,
            pub processed: u8,
        }

        let res =
            query_as!(Row, "SELECT * from battlereports WHERE id = ?", report_id)
            .fetch_optional(&self.pool)
            .await?;

        let res = res.map(|e: Row| BattlelogBattlereport {
            id: e.id,
            duration: e.duration,
            winner: e.winner,
            server_id: e.server_id,
            map: e.map,
            mode: e.mode,
            created_at: e.created_at,
            processed: e.processed,
        });

        Ok(res)
    }

    pub async fn insert_battlereport(&self, battlereport: &BattlelogBattlereport) -> anyhow::Result<bool> {
        self.insert_battlereport_private(None, battlereport).await
    }

    pub async fn insert_battlereport_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, battlereport: &BattlelogBattlereport) -> anyhow::Result<bool> {
        self.insert_battlereport_private(Some(transaction), battlereport).await
    }

    async fn insert_battlereport_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, battlereport: &BattlelogBattlereport) -> anyhow::Result<bool> {
        let mut res = 0;

        let query = query!(r#"INSERT INTO battlereports (id, duration, winner, server_id, map, mode, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"#, 
            battlereport.id, 
            battlereport.duration, 
            battlereport.winner, 
            battlereport.server_id,
            battlereport.map,
            battlereport.mode,
            battlereport.created_at);
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

        Ok(res > 0)
    }

    pub async fn upsert_battlereport(&self, transaction: &mut Transaction<'_, MySql>, battlereport: &BattlelogBattlereport) -> anyhow::Result<bool> {
        let query = query!(r#"INSERT IGNORE INTO battlereports (id, duration, winner, server_id, map, mode, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"#, 
            battlereport.id, 
            battlereport.duration, 
            battlereport.winner, 
            battlereport.server_id,
            battlereport.map,
            battlereport.mode,
            battlereport.created_at);
        
        let res = query
                .execute(&mut *transaction)
                .await?
                .last_insert_id();

        Ok(res > 0)
    }

    pub async fn update_battlereport(&self, battlereport: &BattlelogBattlereport) -> anyhow::Result<bool> {
        self.update_battlereport_private(None, battlereport).await
    }

    pub async fn update_battlereport_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, battlereport: &BattlelogBattlereport) -> anyhow::Result<bool> {
        self.update_battlereport_private(Some(transaction), battlereport).await
    }

    async fn update_battlereport_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, battlereport: &BattlelogBattlereport) -> anyhow::Result<bool> {
        let mut res: u64 = 0;

        let query = query!(r#"UPDATE battlereports SET duration = ?, winner = ?, server_id = ?, map = ?, mode = ?, created_at = ?, processed = ? WHERE id = ?"#,
            battlereport.duration, 
            battlereport.winner, 
            battlereport.server_id,
            battlereport.map,
            battlereport.mode,
            battlereport.created_at, 
            battlereport.processed, 
            battlereport.id);
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