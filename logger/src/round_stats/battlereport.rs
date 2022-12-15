use core::time;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow};
use battlelog::{BattlereportResponse, battlereport, playerreport, GameReport, warsawbattlereportspopulatemore};
use futures::future::join_all;

use crate::database::battlelog::battlereports::BattlelogBattlereport;
use crate::database::battlelog::context::BattlelogContext;
use crate::database::battlelog::personas::BattlelogPersona;
use crate::database::battlelog::playerreports::BattlelogPlayerreport;
use crate::database::battlelog::servers::BattlelogServer;
use crate::endpoints::battlereport::BattleReportAddingResponse;

pub async fn read_brr(path: &str) -> anyhow::Result<()> {
    let uri = get_db_coninfo()?;
    let db = BattlelogContext::connect(uri).await?;

    let chunk_size = 500;
    let mut lines_processed = 0;
    let mut reports_processed = 0;
    let mut battle_reports: Vec<BattlereportResponse> = Vec::new();
    let mut server: Option<BattlelogServer> = None;
    if let Ok(lines) = read_lines(path) {
        for line in lines {
            if let Ok(br) = line {
                let split: Vec<&str> = br.splitn(4, ' ').collect();
                lines_processed += 1;

                if split[0].eq_ignore_ascii_case("#IX#") {
                    continue;
                }

                // info!("Processing {} - {}", split[1], split[2]);
                match serde_json::from_str::<BattlereportResponse>(&split[3]) {
                    Ok(data) => {
                        // insert_player_report(&db, &data).await?;
                        battle_reports.push(data);
                        reports_processed += 1;
                    },
                    Err(err) => {
                        error!("{}: {}", &br, err)
                    },
                };

                if battle_reports.len() >= chunk_size {
                    let results = build_queries(&db, server, &battle_reports).await?;
                    server = results.server.clone();
                    info!("{} lines processed", lines_processed);
                    upsert_queries(&db, &results).await?;
                    info!("{} reports upserted", reports_processed);
                    battle_reports.clear();
                }
            }
        }

        let results = build_queries(&db, server, &battle_reports).await?;
        server = results.server.clone();
        info!("{} lines processed", lines_processed);
        upsert_queries(&db, &results).await?;
        info!("{} reports upserted", reports_processed);
        battle_reports.clear();
    }

    Ok(())
}

pub async fn read_battlereport_ids(path: &str) -> anyhow::Result<()> {
    let uri = get_db_coninfo()?;
    let db = BattlelogContext::connect(uri).await?;

    let mut reports_processed = 0;
    if let Ok(lines) = read_lines(path) {
        for line in lines {
            if let Ok(report_id) = line {
                info!("Processing {}", report_id);
                match add_battlereport_by_id_private(&db, &report_id).await {
                    Ok(data) => {
                        reports_processed += 1;
                        trace!("{:?}", data);
                        if data.success {
                            info!("Success for {}", report_id);
                        }
                        else {
                            info!("Failed for {} with {}", report_id, data.errors.unwrap().join(","));
                        }
                    },
                    Err(err) => {
                        error!("{} failed to process because {}", &report_id, err);
                        info!("Trying {} again after 500ms delay", report_id);
                        thread::sleep(time::Duration::from_millis(500));
                        match add_battlereport_by_id_private(&db, &report_id).await {
                            Ok(data) => {
                                reports_processed += 1;
                                trace!("{:?}", data);
                                if data.success {
                                    info!("Success for {}", report_id);
                                }
                                else {
                                    info!("Failed for {} with {}", report_id, data.errors.unwrap().join(","));
                                }
                            },
                            Err(err) => {
                                error!("{} failed to process because {}", &report_id, err)
                            },
                        };
                    },
                };
            }
        }
    }

    db.close().await;

    Ok(())
}

pub async fn fetch_battlereports_for_user(persona_id: &str, timestamp: &Option<String>) -> anyhow::Result<Vec<GameReport>> {
    let mut game_reports: Vec<GameReport> = Vec::new();

    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let time_now = since_the_epoch.as_secs().to_string();
    let mut timestamp = timestamp.clone();
    let mut n = 0;
    while n < 6 {
        let timestamp_used = if timestamp.is_some() {
            timestamp.as_ref().unwrap().clone()
        } else {
            time_now.clone()
        };
        match warsawbattlereportspopulatemore(&persona_id, &timestamp_used).await {
            Ok(data) => {
                trace!("{:?}", data);

                // If query wasn't successfull
                if data.r#type != "success" {
                    info!("More fetch failed with status {}", data.r#type);
                    n += 1;
                    continue;
                }

                // If query didn't contain any reports
                if data.data.game_reports.is_none() {
                    info!("Game Reports array empty for {} at {:?}", &persona_id, &timestamp);
                    n += 1;
                    thread::sleep(time::Duration::from_millis(500));
                    continue;
                }

                let reports = data.data.game_reports.unwrap();

                let len = reports.len();
                if len == 0 {
                    info!("Game Reports array contains 0 reports for {} at {:?}", &persona_id, &timestamp);
                    n += 1;
                    thread::sleep(time::Duration::from_millis(500));
                    continue;
                }

                for report in reports {
                    game_reports.push(report);
                }

                info!("{} Game Reports fetched for {} at {:?}", len, &persona_id, &timestamp);
                match game_reports.last() {
                    Some(last_report) => {
                        timestamp = Some(last_report.created_at.to_string());
                    },
                    None => {
                        error!("{} at {:?} stopping because couldn't get the next timestamp", &persona_id, &timestamp);
                        break;
                    },
                }
            },
            Err(err) => {
                error!("{} at {:?} failed to fetch more reports with reason: {}", &persona_id, &timestamp, err);
                break;
            },
        };
    }

    Ok(game_reports)
}

pub async fn add_battlereport_by_id(report_id: &str) -> anyhow::Result<BattleReportAddingResponse> {
    let uri = get_db_coninfo()?;
    let db = BattlelogContext::connect(uri).await?;

    match add_battlereport_by_id_private(&db, &report_id).await {
        Ok(result) => {
            db.close().await;
            Ok(result)
        },
        Err(err) => {
            db.close().await;
            Err(err)
        },
    }
}

async fn add_battlereport_by_id_private(db: &BattlelogContext, report_id: &str) -> anyhow::Result<BattleReportAddingResponse> {
    let report_id_u64 = report_id.parse::<u64>()?;
    let br = db.get_battlereport_by_report_id(report_id_u64).await?;

    if br.is_some() && br.unwrap().processed > 0 {
        info!("Tried to add already existing BattleReport with id: {}", report_id_u64);
        return Ok(BattleReportAddingResponse { 
            success: true, 
            report: BattlelogBattlereport { id: report_id_u64, duration: 0, winner: 0, server_id: 0, map: "".to_string(), mode: "".to_string(), created_at: 0, processed: 0 },
            errors: None,
        });
    }

    let report = battlereport(report_id).await?;

    // Upsert the server
    let server_id: i32;
    let server = db.get_server_by_server_guid(&report.game_server.guid.as_ref().unwrap()).await?;
    if server.is_none() {
        server_id = db.insert_server( 
            &BattlelogServer::new(report.game_server.name.as_ref().unwrap_or(&"".to_string()).to_string(), report.game_server.guid.as_ref().unwrap().to_string())).await? as i32;
    }
    else {
        let mut server = server.unwrap();
        server_id = server.id;
        // Update the name
        server.name = report.game_server.name.as_ref().unwrap_or(&server.name).to_string();
        db.update_server(&server).await?;
    }

    // Inser the battlereport if missing
    let mut transaction = db.begin_transaction().await?;
    db.upsert_battlereport(&mut transaction, &BattlelogBattlereport::from_battlereport_response(&report, server_id)).await?;
    transaction.commit().await?;

    if report.players.is_none() {
        error!("No players in the BattleReport with id: {}", report_id_u64);
        return Err(anyhow!("No players in the BattleReport with id: {}", report_id_u64));
    }

    let players_data = report.players.as_ref().unwrap();
    let persona_ids: Vec<String> = players_data.keys().map(|p| p.to_string()).collect();
    let persona_ids: Vec<&str> = persona_ids.iter().map(std::ops::Deref::deref).collect();
    let mut player_report_fetches = Vec::new();
    for persona_id in persona_ids {
        player_report_fetches.push(playerreport(report_id, &persona_id));
    }

    // Upsert personas
    let work = join_all(player_report_fetches).await;

    let mut errors: Vec<String> = Vec::new();

    let mut transaction = db.begin_transaction().await?;
    for response in work {
        match response {
            Ok(data) => {
                let persona_id = data.persona_id.parse::<u64>().unwrap();
                let persona = db.get_persona_by_persona_id(persona_id).await?;
                let gravatar_md5 = get_gravatar(&report, persona_id);
                
                let battlelog_persona = BattlelogPersona::from_playerreport_response_with_gravatar(&data, gravatar_md5);
                if persona.is_none() {
                    // Insert
                    db.upsert_persona(&mut transaction, &battlelog_persona).await?;
                }
                else if data.persona.is_some() {
                    // Update
                    let mut persona = persona.unwrap();
                    persona.name = battlelog_persona.name;
                    persona.clan_tag = battlelog_persona.clan_tag;
                    persona.gravatar_md5 = battlelog_persona.gravatar_md5;
                    db.update_persona_with_transaction(&mut transaction, &persona).await?;
                }

                // Generate player report
                let report_player = report.get_player_by_personaid(persona_id).unwrap();

                db.upsert_playerreport(&mut transaction, &BattlelogPlayerreport::from_response_and_report(report_id_u64, &data, &report_player)).await?;
            },
            Err(err) => {
                errors.push(err.to_string());
                warn!("Failed to get player report in report {}", report_id_u64);
            },
        }
    }
    transaction.commit().await?;

    let mut battlereport = db.get_battlereport_by_report_id(report_id_u64).await?.unwrap();

    if errors.len() > 0 {
        Ok(BattleReportAddingResponse { 
            success: false, 
            report: battlereport,
            errors: Some(errors),
        })
    }
    else {
        battlereport.processed = 1;
        let result = db.update_battlereport(&battlereport).await?;
        if !result {
            Ok(BattleReportAddingResponse { 
                success: result, 
                report: battlereport,
                errors: Some(vec!["Failed to update processed flag".to_string()]),
            })
        }
        else {
            Ok(BattleReportAddingResponse { 
                success: result, 
                report: battlereport,
                errors: None,
            })
        }
    }
}

fn get_db_coninfo() -> anyhow::Result<String> {
    dotenv::dotenv()?;
    let uri = std::env::var("DATABASE_URL")
        .context("Need to specify Battlefield db URI via env var, for example DATABASE_URL=\"mysql://username:password@host/database\"")?;
    Ok(uri)
}

async fn insert_player_report(db: &BattlelogContext, battlereport: &BattlereportResponse) -> anyhow::Result<()> {
    let battlereport_id = battlereport.id.parse::<u64>();
    if battlereport_id.is_err() {
        warn!("Report ID invalid in the report");
        return Ok(());
    }
    let battlereport_id = battlereport_id.unwrap();

    if battlereport.game_server.guid.is_none() {
        warn!("Server GUID missing from the report");
        return Ok(());
    }

    if battlereport.player_report.is_none() {
        warn!("Player report missing from the report");
        return Ok(());
    }

    // Insert server if missing
    let server = db.get_server_by_server_guid(&battlereport.game_server.guid.as_ref().unwrap()).await?;

    let server_id: i32;
    if server.is_none() {
        server_id = db.insert_server( 
            &BattlelogServer::new(battlereport.game_server.name.as_ref().unwrap_or(&"".to_string()).to_string(), battlereport.game_server.guid.as_ref().unwrap().to_string())).await? as i32;
    }
    else {
        server_id = server.unwrap().id;
    }

    // Insert player if missing
    let mut persona_id: u64;
    if battlereport.player_report.as_ref().unwrap().persona.is_some() {
        persona_id = battlereport.player_report.as_ref().unwrap().persona.as_ref().unwrap().persona_id;
    }
    else {
        persona_id = battlereport.player_report.as_ref().unwrap().persona_id.parse::<u64>()?;
    }

    let persona = db.get_persona_by_persona_id(persona_id).await?;
    if persona.is_none() {
        if battlereport.player_report.as_ref().unwrap().persona.is_some() {
            let persona = battlereport.player_report.as_ref().unwrap().persona.as_ref().unwrap();
            persona_id = db.insert_persona( 
                &BattlelogPersona::new(persona_id, Some(persona.persona_name.to_string()), persona.clan_tag.clone(), get_gravatar(battlereport, persona_id), true)).await?;
        }
        else {
            persona_id = db.insert_persona( 
                &BattlelogPersona::new(persona_id, None, None, None, false)).await?;
        }
    }

    // Insert battle report
    let br = db.get_battlereport_by_report_id(battlereport_id).await?;
    if br.is_none() && !db.insert_battlereport(&BattlelogBattlereport::new(battlereport_id, battlereport.duration as u32, get_winner(&battlereport), server_id, battlereport.game_server.map.clone().unwrap(), battlereport.game_server.map_mode.clone().unwrap(), battlereport.created_at, 1)).await? {
        warn!("Battlereport not found and failed to insert");
        return Ok(());
    }

    // Insert player report
    let player_report = db.get_playerreport_by_report_id_and_persona_id(battlereport_id, persona_id).await?;
    let player = battlereport.get_player_by_personaid(persona_id).unwrap();
    let p_report = battlereport.player_report.as_ref().unwrap();
    if player_report.is_none() && !db.insert_playerreport(&BattlelogPlayerreport::new(battlereport_id,
        persona_id,
        player.kills,
        player.deaths.into(),
        p_report.stats.shots_hit,
        p_report.stats.shots_fired,
        p_report.stats.vehicle_destroyed,
        p_report.stats.assists,
        p_report.stats.get_spm(),
        p_report.stats.kd_ratio,
        p_report.stats.skill,
        p_report.stats.vehicle_assists,
        p_report.stats.accuracy,
        p_report.scores.sc_unlock,
        p_report.scores.sc_bomber,
        p_report.scores.sc_vehiclesh,
        p_report.scores.sc_vehicleajet,
        p_report.scores.sc_engineer,
        p_report.scores.sc_commander,
        p_report.scores.sc_assault,
        p_report.scores.vehicle,
        p_report.scores.sc_vehicleaa,
        p_report.scores.sc_award,
        p_report.scores.sc_vehicleifv,
        p_report.scores.sc_recon,
        p_report.scores.sc_vehicleah,
        p_report.scores.sc_support,
        p_report.scores.sc_vehiclesjet,
        p_report.scores.total,
        p_report.scores.sc_vehiclembt,
        p_report.scores.sc_vehicleaboat,
        player.heals,
        player.revives,
        player.team,
        player.kill_streak,
        player.squad_id,
        player.accuracy as f32,
        player.dnf,
        player.is_commander,
        player.is_soldier
    )).await? {
        warn!("Battlereport not found and failed to insert");
        return Ok(());
    }

    Ok(())
}

struct BattlereportQueries {
    pub server: Option<BattlelogServer>,
    pub personas: Vec<BattlelogPersona>,
    pub battlereports: Vec<BattlelogBattlereport>,
    pub playerreports: Vec<BattlelogPlayerreport>,
}

impl BattlereportQueries {
    pub fn new() -> Self {
        Self {
            server: None,
            personas: Vec::new(),
            battlereports: Vec::new(),
            playerreports: Vec::new(),
        }
    }
}

async fn build_queries(db: &BattlelogContext, server_cached: Option<BattlelogServer>, battlereports: &Vec<BattlereportResponse>) -> anyhow::Result<BattlereportQueries> {
    let mut queries = BattlereportQueries::new();

    for battlereport in battlereports.iter() {
        let battlereport_id = battlereport.id.parse::<u64>();
        if battlereport_id.is_err() {
            warn!("Report ID invalid in the report");
            return Err(anyhow!("Report ID invalid in the report"));
        }
        let battlereport_id = battlereport_id.unwrap();
    
        if battlereport.game_server.guid.is_none() {
            warn!("Server GUID missing from the report");
            return Err(anyhow!("Server GUID missing from the report"));
        }
    
        if battlereport.player_report.is_none() {
            warn!("Player report missing from the report");
            return Err(anyhow!("Player report missing from the report"));
        }
    
        // Insert server if missing   
        let server_id: i32;
        if server_cached.is_some() && server_cached.as_ref().unwrap().guid.eq(battlereport.game_server.guid.as_ref().unwrap()) {
            server_id = server_cached.as_ref().unwrap().id;
        }
        else {
            let server = db.get_server_by_server_guid(&battlereport.game_server.guid.as_ref().unwrap()).await?;
            queries.server = server.clone();

            if server.is_none() {
                server_id = db.insert_server( 
                    &BattlelogServer::new(battlereport.game_server.name.as_ref().unwrap_or(&"".to_string()).to_string(), battlereport.game_server.guid.as_ref().unwrap().to_string())).await? as i32;
            }
            else {
                server_id = server.unwrap().id;
            }
        }

    
        // Insert player if missing
        let persona_id: u64;
        if battlereport.player_report.as_ref().unwrap().persona.is_some() {
            persona_id = battlereport.player_report.as_ref().unwrap().persona.as_ref().unwrap().persona_id;
        }
        else {
            persona_id = battlereport.player_report.as_ref().unwrap().persona_id.parse::<u64>()?;
        }
    
        if battlereport.player_report.as_ref().unwrap().persona.is_some() {
            let persona = battlereport.player_report.as_ref().unwrap().persona.as_ref().unwrap();
            queries.personas.push(BattlelogPersona::new(persona_id, Some(persona.persona_name.to_string()), persona.clan_tag.clone(), get_gravatar(battlereport, persona_id), true));
        }
        else {
            queries.personas.push(BattlelogPersona::new(persona_id, None, None, None, false));
        }
    
        // Insert battle report
        queries.battlereports.push(BattlelogBattlereport::new(battlereport_id, battlereport.duration as u32, get_winner(&battlereport), server_id, battlereport.game_server.map.clone().unwrap(), battlereport.game_server.map_mode.clone().unwrap(), battlereport.created_at, 1));

        // Generate player report
        let player = battlereport.get_player_by_personaid(persona_id).unwrap();
        let p_report = battlereport.player_report.as_ref().unwrap();

        queries.playerreports.push(BattlelogPlayerreport::new(battlereport_id,
            persona_id,
            player.kills,
            player.deaths.into(),
            p_report.stats.shots_hit,
            p_report.stats.shots_fired,
            p_report.stats.vehicle_destroyed,
            p_report.stats.assists,
            p_report.stats.get_spm(),
            p_report.stats.kd_ratio,
            p_report.stats.skill,
            p_report.stats.vehicle_assists,
            p_report.stats.accuracy,
            p_report.scores.sc_unlock,
            p_report.scores.sc_bomber,
            p_report.scores.sc_vehiclesh,
            p_report.scores.sc_vehicleajet,
            p_report.scores.sc_engineer,
            p_report.scores.sc_commander,
            p_report.scores.sc_assault,
            p_report.scores.vehicle,
            p_report.scores.sc_vehicleaa,
            p_report.scores.sc_award,
            p_report.scores.sc_vehicleifv,
            p_report.scores.sc_recon,
            p_report.scores.sc_vehicleah,
            p_report.scores.sc_support,
            p_report.scores.sc_vehiclesjet,
            p_report.scores.total,
            p_report.scores.sc_vehiclembt,
            p_report.scores.sc_vehicleaboat,
            player.heals,
            player.revives,
            player.team,
            player.kill_streak,
            player.squad_id,
            player.accuracy as f32,
            player.dnf,
            player.is_commander,
            player.is_soldier
        ));
    }

    Ok(queries)
}

async fn upsert_queries(db: &BattlelogContext, queries: &BattlereportQueries) -> anyhow::Result<()> {
    // Upsert personas
    let mut transaction = db.begin_transaction().await?;
    for persona in queries.personas.iter() {
        db.upsert_persona(&mut transaction, &persona).await?;
    }
    transaction.commit().await?;

    // Upsert battle reports
    let mut transaction = db.begin_transaction().await?;
    for battlereport in queries.battlereports.iter() {
        db.upsert_battlereport(&mut transaction, &battlereport).await?;
    }
    transaction.commit().await?;

    // Upsert player reports
    let mut transaction = db.begin_transaction().await?;
    for playerreport in queries.playerreports.iter() {
        db.upsert_playerreport(&mut transaction, &playerreport).await?;
    }
    transaction.commit().await?;

    Ok(())
}

fn get_winner(battlereport: &BattlereportResponse) -> i8 {
    for team in battlereport.teams.iter() {
        let team = team.1;
        if team.is_winner {
            return team.id;
        }
    }

    -1
}

fn get_gravatar(battlereport: &BattlereportResponse, persona_id: u64) -> Option<String> {
    let player = battlereport.get_player_by_personaid(persona_id);
    if player.is_none() {
        return None;
    }

    let player = player.unwrap();
    if player.persona.is_none() {
        return None;
    }

    let persona = player.persona.as_ref().unwrap();
    let user = persona.user.as_ref();
    if user.is_none() {
        return None;
    }

    user.unwrap().gravatar_md5.clone()
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
