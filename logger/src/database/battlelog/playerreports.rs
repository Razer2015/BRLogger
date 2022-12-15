use battlelog::{ReportPlayer, PlayerreportResponse};
use serde::Serialize;
use sqlx::{query_as, query, MySql, Transaction};

use super::context::BattlelogContext;

#[derive(Debug, Serialize)]
pub struct BattlelogPlayerreport {
    pub report_id: u64,
    pub persona_id: u64,
    pub kills: u32,
    pub deaths: u32,
    pub shots_hit: f32,
    pub shots_fired: f32,
    pub vehicle_destroyed: u32,
    pub assists: u32,
    pub spm: u32,
    pub kd_ratio: f32,
    pub skill: i32,
    pub vehicle_assists: u32,
    pub accuracy: u32,
    pub sc_unlock: u32,
    pub sc_bomber: u32,
    pub sc_vehiclesh: u32,
    pub sc_vehicleajet: u32,
    pub sc_engineer: u32,
    pub sc_commander: u32,
    pub sc_assault: u32,
    pub vehicle: u32,
    pub sc_vehicleaa: u32,
    pub sc_award: u32,
    pub sc_vehicleifv: u32,
    pub sc_recon: u32,
    pub sc_vehicleah: u32,
    pub sc_support: u32,
    pub sc_vehiclesjet: u32,
    pub total: u32,
    pub sc_vehiclembt: u32,
    pub sc_vehicleaboat: u32,
    pub heals: u16,
    pub revives: u16,
    pub team: i8,
    pub kill_streak: u16,
    pub squad_id: i8,
    pub accuracy_detailed: f32,
    pub dnf: bool,
    pub is_commander: bool,
    pub is_soldier: bool,
}

impl BattlelogPlayerreport {
    pub fn new(report_id: u64,
            persona_id: u64,
            kills: u32,
            deaths: u32,
            shots_hit: f32,
            shots_fired: f32,
            vehicle_destroyed: u32,
            assists: u32,
            spm: u32,
            kd_ratio: f32,
            skill: i32,
            vehicle_assists: u32,
            accuracy: u32,
            sc_unlock: u32,
            sc_bomber: u32,
            sc_vehiclesh: u32,
            sc_vehicleajet: u32,
            sc_engineer: u32,
            sc_commander: u32,
            sc_assault: u32,
            vehicle: u32,
            sc_vehicleaa: u32,
            sc_award: u32,
            sc_vehicleifv: u32,
            sc_recon: u32,
            sc_vehicleah: u32,
            sc_support: u32,
            sc_vehiclesjet: u32,
            total: u32,
            sc_vehiclembt: u32,
            sc_vehicleaboat: u32,
            heals: u16,
            revives: u16,
            team: i8,
            kill_streak: u16,
            squad_id: i8,
            accuracy_detailed: f32,
            dnf: bool,
            is_commander: bool,
            is_soldier: bool) -> Self {
        Self { 
            report_id,
            persona_id,
            kills,
            deaths,
            shots_hit,
            shots_fired,
            vehicle_destroyed,
            assists,
            spm,
            kd_ratio,
            skill,
            vehicle_assists,
            accuracy,
            sc_unlock,
            sc_bomber,
            sc_vehiclesh,
            sc_vehicleajet,
            sc_engineer,
            sc_commander,
            sc_assault,
            vehicle,
            sc_vehicleaa,
            sc_award,
            sc_vehicleifv,
            sc_recon,
            sc_vehicleah,
            sc_support,
            sc_vehiclesjet,
            total,
            sc_vehiclembt,
            sc_vehicleaboat,
            heals,
            revives,
            team,
            kill_streak,
            squad_id,
            accuracy_detailed,
            dnf,
            is_commander,
            is_soldier,
        }
    }

    pub fn from_response_and_report(report_id: u64, response: &PlayerreportResponse, report_player: &ReportPlayer) -> Self {
        let persona_id = response.persona_id.parse::<u64>().unwrap();

        BattlelogPlayerreport::new(report_id,
            persona_id,
            report_player.kills,
            report_player.deaths.into(),
            response.stats.shots_hit,
            response.stats.shots_fired,
            response.stats.vehicle_destroyed,
            response.stats.assists,
            response.stats.get_spm(),
            response.stats.kd_ratio,
            response.stats.skill,
            response.stats.vehicle_assists,
            response.stats.accuracy,
            response.scores.sc_unlock,
            response.scores.sc_bomber,
            response.scores.sc_vehiclesh,
            response.scores.sc_vehicleajet,
            response.scores.sc_engineer,
            response.scores.sc_commander,
            response.scores.sc_assault,
            response.scores.vehicle,
            response.scores.sc_vehicleaa,
            response.scores.sc_award,
            response.scores.sc_vehicleifv,
            response.scores.sc_recon,
            response.scores.sc_vehicleah,
            response.scores.sc_support,
            response.scores.sc_vehiclesjet,
            response.scores.total,
            response.scores.sc_vehiclembt,
            response.scores.sc_vehicleaboat,
            report_player.heals,
            report_player.revives,
            report_player.team,
            report_player.kill_streak,
            report_player.squad_id,
            report_player.accuracy as f32,
            report_player.dnf,
            report_player.is_commander,
            report_player.is_soldier
        )
    }
}

impl BattlelogContext {
    pub async fn get_playerreport_by_report_id_and_persona_id(&self, report_id: u64, persona_id: u64) -> Result<Option<BattlelogPlayerreport>, sqlx::Error> {
        pub struct Row {
            pub report_id: u64,
            pub persona_id: u64,
            pub kills: u32,
            pub deaths: u32,
            pub shots_hit: f32,
            pub shots_fired: f32,
            pub vehicle_destroyed: u32,
            pub assists: u32,
            pub spm: u32,
            pub kd_ratio: f32,
            pub skill: i32,
            pub vehicle_assists: u32,
            pub accuracy: u32,
            pub sc_unlock: u32,
            pub sc_bomber: u32,
            pub sc_vehiclesh: u32,
            pub sc_vehicleajet: u32,
            pub sc_engineer: u32,
            pub sc_commander: u32,
            pub sc_assault: u32,
            pub vehicle: u32,
            pub sc_vehicleaa: u32,
            pub sc_award: u32,
            pub sc_vehicleifv: u32,
            pub sc_recon: u32,
            pub sc_vehicleah: u32,
            pub sc_support: u32,
            pub sc_vehiclesjet: u32,
            pub total: u32,
            pub sc_vehiclembt: u32,
            pub sc_vehicleaboat: u32,
            pub heals: u16,
            pub revives: u16,
            pub team: i8,
            pub kill_streak: u16,
            pub squad_id: i8,
            pub accuracy_detailed: f32,
            pub dnf: u8,
            pub is_commander: u8,
            pub is_soldier: u8,
        }

        let res =
            query_as!(Row, "SELECT * from playerreports WHERE report_id = ? AND persona_id = ?", report_id, persona_id)
            .fetch_optional(&self.pool)
            .await?;

        let res = res.map(|e: Row| BattlelogPlayerreport {
            report_id: e.report_id,
            persona_id: e.persona_id,
            kills: e.kills,
            deaths: e.deaths,
            shots_hit: e.shots_hit,
            shots_fired: e.shots_fired,
            vehicle_destroyed: e.vehicle_destroyed,
            assists: e.assists,
            spm: e.spm,
            kd_ratio: e.kd_ratio,
            skill: e.skill,
            vehicle_assists: e.vehicle_assists,
            accuracy: e.accuracy,
            sc_unlock: e.sc_unlock,
            sc_bomber: e.sc_bomber,
            sc_vehiclesh: e.sc_vehiclesh,
            sc_vehicleajet: e.sc_vehicleajet,
            sc_engineer: e.sc_engineer,
            sc_commander: e.sc_commander,
            sc_assault: e.sc_assault,
            vehicle: e.vehicle,
            sc_vehicleaa: e.sc_vehicleaa,
            sc_award: e.sc_award,
            sc_vehicleifv: e.sc_vehicleifv,
            sc_recon: e.sc_recon,
            sc_vehicleah: e.sc_vehicleah,
            sc_support: e.sc_support,
            sc_vehiclesjet: e.sc_vehiclesjet,
            total: e.total,
            sc_vehiclembt: e.sc_vehiclembt,
            sc_vehicleaboat: e.sc_vehicleaboat,
            heals: e.heals,
            revives: e.revives,
            team: e.team,
            kill_streak: e.kill_streak,
            squad_id: e.squad_id,
            accuracy_detailed: e.accuracy_detailed,
            dnf: e.dnf == 1,
            is_commander: e.is_commander == 1,
            is_soldier: e.is_soldier == 1,
        });

        Ok(res)
    }

    pub async fn insert_playerreport(&self, playerreport: &BattlelogPlayerreport) -> anyhow::Result<bool> {
        self.insert_playerreport_private(None, playerreport).await
    }

    pub async fn insert_playerreport_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, playerreport: &BattlelogPlayerreport) -> anyhow::Result<bool> {
        self.insert_playerreport_private(Some(transaction), playerreport).await
    }

    async fn insert_playerreport_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, playerreport: &BattlelogPlayerreport) -> anyhow::Result<bool> {
        let mut res = 0;

        let query = query!(r#"INSERT INTO playerreports (report_id, persona_id, kills, deaths, shots_hit, shots_fired, vehicle_destroyed, assists, spm, kd_ratio, skill, vehicle_assists, accuracy, sc_unlock, sc_bomber, sc_vehiclesh, sc_vehicleajet, sc_engineer, sc_commander, sc_assault, vehicle, sc_vehicleaa, sc_award, sc_vehicleifv, sc_recon, sc_vehicleah, sc_support, sc_vehiclesjet, total, sc_vehiclembt, sc_vehicleaboat, heals, revives, team, kill_streak, squad_id, accuracy_detailed, dnf, is_commander, is_soldier) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#, 
            playerreport.report_id,
            playerreport.persona_id,
            playerreport.kills,
            playerreport.deaths,
            playerreport.shots_hit,
            playerreport.shots_fired,
            playerreport.vehicle_destroyed,
            playerreport.assists,
            playerreport.spm,
            playerreport.kd_ratio,
            playerreport.skill,
            playerreport.vehicle_assists,
            playerreport.accuracy,
            playerreport.sc_unlock,
            playerreport.sc_bomber,
            playerreport.sc_vehiclesh,
            playerreport.sc_vehicleajet,
            playerreport.sc_engineer,
            playerreport.sc_commander,
            playerreport.sc_assault,
            playerreport.vehicle,
            playerreport.sc_vehicleaa,
            playerreport.sc_award,
            playerreport.sc_vehicleifv,
            playerreport.sc_recon,
            playerreport.sc_vehicleah,
            playerreport.sc_support,
            playerreport.sc_vehiclesjet,
            playerreport.total,
            playerreport.sc_vehiclembt,
            playerreport.sc_vehicleaboat,
            playerreport.heals,
            playerreport.revives,
            playerreport.team,
            playerreport.kill_streak,
            playerreport.squad_id,
            playerreport.accuracy_detailed,
            playerreport.dnf,
            playerreport.is_commander,
            playerreport.is_soldier);
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

    pub async fn upsert_playerreport(&self, transaction: &mut Transaction<'_, MySql>, playerreport: &BattlelogPlayerreport) -> anyhow::Result<bool> {
        let query = query!(r#"INSERT IGNORE INTO playerreports (report_id, persona_id, kills, deaths, shots_hit, shots_fired, vehicle_destroyed, assists, spm, kd_ratio, skill, vehicle_assists, accuracy, sc_unlock, sc_bomber, sc_vehiclesh, sc_vehicleajet, sc_engineer, sc_commander, sc_assault, vehicle, sc_vehicleaa, sc_award, sc_vehicleifv, sc_recon, sc_vehicleah, sc_support, sc_vehiclesjet, total, sc_vehiclembt, sc_vehicleaboat, heals, revives, team, kill_streak, squad_id, accuracy_detailed, dnf, is_commander, is_soldier) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#, 
            playerreport.report_id,
            playerreport.persona_id,
            playerreport.kills,
            playerreport.deaths,
            playerreport.shots_hit,
            playerreport.shots_fired,
            playerreport.vehicle_destroyed,
            playerreport.assists,
            playerreport.spm,
            playerreport.kd_ratio,
            playerreport.skill,
            playerreport.vehicle_assists,
            playerreport.accuracy,
            playerreport.sc_unlock,
            playerreport.sc_bomber,
            playerreport.sc_vehiclesh,
            playerreport.sc_vehicleajet,
            playerreport.sc_engineer,
            playerreport.sc_commander,
            playerreport.sc_assault,
            playerreport.vehicle,
            playerreport.sc_vehicleaa,
            playerreport.sc_award,
            playerreport.sc_vehicleifv,
            playerreport.sc_recon,
            playerreport.sc_vehicleah,
            playerreport.sc_support,
            playerreport.sc_vehiclesjet,
            playerreport.total,
            playerreport.sc_vehiclembt,
            playerreport.sc_vehicleaboat,
            playerreport.heals,
            playerreport.revives,
            playerreport.team,
            playerreport.kill_streak,
            playerreport.squad_id,
            playerreport.accuracy_detailed,
            playerreport.dnf,
            playerreport.is_commander,
            playerreport.is_soldier);
        let res = query
            .execute(&mut *transaction)
            .await?
            .last_insert_id();

        Ok(res > 0)
    }

    pub async fn update_playerreport(&self, playerreport: &BattlelogPlayerreport) -> anyhow::Result<bool> {
        self.update_playerreport_private(None, playerreport).await
    }

    pub async fn update_playerreport_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, playerreport: &BattlelogPlayerreport) -> anyhow::Result<bool> {
        self.update_playerreport_private(Some(transaction), playerreport).await
    }

    async fn update_playerreport_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, playerreport: &BattlelogPlayerreport) -> anyhow::Result<bool> {
        let mut res: u64 = 0;

        let query = query!(r#"UPDATE playerreports SET kills = ?, deaths = ?, shots_hit = ?, shots_fired = ?, vehicle_destroyed = ?, assists = ?, spm = ?, kd_ratio = ?, skill = ?, vehicle_assists = ?, accuracy = ?, sc_unlock = ?, sc_bomber = ?, sc_vehiclesh = ?, sc_vehicleajet = ?, sc_engineer = ?, sc_commander = ?, sc_assault = ?, vehicle = ?, sc_vehicleaa = ?, sc_award = ?, sc_vehicleifv = ?, sc_recon = ?, sc_vehicleah = ?, sc_support = ?, sc_vehiclesjet = ?, total = ?, sc_vehiclembt = ?, sc_vehicleaboat = ?, heals = ?, revives = ?, team = ?, kill_streak = ?, squad_id = ?, accuracy_detailed = ?, dnf = ?, is_commander = ?, is_soldier = ? 
            WHERE report_id = ? AND persona_id = ?"#,
            playerreport.kills,
            playerreport.deaths,
            playerreport.shots_hit,
            playerreport.shots_fired,
            playerreport.vehicle_destroyed,
            playerreport.assists,
            playerreport.spm,
            playerreport.kd_ratio,
            playerreport.skill,
            playerreport.vehicle_assists,
            playerreport.accuracy,
            playerreport.sc_unlock,
            playerreport.sc_bomber,
            playerreport.sc_vehiclesh,
            playerreport.sc_vehicleajet,
            playerreport.sc_engineer,
            playerreport.sc_commander,
            playerreport.sc_assault,
            playerreport.vehicle,
            playerreport.sc_vehicleaa,
            playerreport.sc_award,
            playerreport.sc_vehicleifv,
            playerreport.sc_recon,
            playerreport.sc_vehicleah,
            playerreport.sc_support,
            playerreport.sc_vehiclesjet,
            playerreport.total,
            playerreport.sc_vehiclembt,
            playerreport.sc_vehicleaboat,
            playerreport.heals,
            playerreport.revives,
            playerreport.team,
            playerreport.kill_streak,
            playerreport.squad_id,
            playerreport.accuracy_detailed,
            playerreport.dnf,
            playerreport.is_commander,
            playerreport.is_soldier,
            playerreport.report_id,
            playerreport.persona_id);
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