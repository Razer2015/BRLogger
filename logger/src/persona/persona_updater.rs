use core::time;
use std::{thread, time::{SystemTime, UNIX_EPOCH}};

use battlelog::{get_users_by_persona_ids, UserResult};
use sqlx::{Transaction, MySql};

use crate::database::battlelog::{context::BattlelogContext, personas::BattlelogPersona, persona_game_expansion::PersonaGameExpansion, game_expansions, persona_info::PersonaInfo};

pub async fn update_personas_without_last_update() -> anyhow::Result<()> {
    let uri = BattlelogContext::get_db_coninfo()?;
    let db = BattlelogContext::connect(uri).await?;

    let personas: Vec<BattlelogPersona> = db.get_personas_without_update().await?;
    let personas: Vec<String> = personas.iter().map(|p| p.id.to_string()).collect();
    info!("{} personas to be checked for premium", personas.len());

    let chunk_size = 100;
    let total_to_process = personas.len();
    let mut total_processed = 0;
    for persona_chunk in personas.chunks(chunk_size) {
        total_processed += persona_chunk.len();
        match get_users_by_persona_ids(persona_chunk.to_vec()).await {
            Ok(results) => {
                if results.len() != chunk_size {
                    warn!("Got {}/{} results", results.len(), chunk_size);
                }
                else {
                    info!("Got {}/{} results", results.len(), chunk_size);
                }

                let mut transaction = db.begin_transaction().await?;
                if let Err(err) = update_persona_infos(&db, &mut transaction, results).await {
                    error!("Error updating personas: {}", err)
                }
                transaction.commit().await?;
                info!("{}/{} personas updated", total_processed, total_to_process);

                thread::sleep(time::Duration::from_millis(100));
            },
            Err(err) => {
                error!("Failed to fetch personas: {}", err)
            },
        }
    }

    Ok(())
}


async fn update_persona_infos(db: &BattlelogContext, transaction: &mut Transaction<'_, MySql>, user_results: Vec<UserResult>) -> anyhow::Result<()> {
    for user_result in user_results {
        let persona_id = user_result.persona_id.parse::<u64>().unwrap();
        trace!("Updating persona {}", persona_id);

        let persona = db.get_persona_by_persona_id(persona_id).await?;

        if persona.is_none() {
            return Err(anyhow::anyhow!("Persona {} not found from database", persona_id));
        }

        let mut persona = persona.unwrap();

        // Insert game expansions
        trace!("Inserting game expansions for {}", persona_id);
        for game_expansion in user_result.game_expansions {
            let game_expansion_id = game_expansion.0.parse::<u64>().unwrap();
            let platforms = game_expansion.1.iter().sum::<i32>() as u32;
            db.upsert_persona_game_expansion(transaction, &PersonaGameExpansion::new(persona_id, game_expansion_id, platforms)).await?;
        }
        
        // Insert/Update persona info
        if user_result.info.is_some() {
            trace!("Updating persona info for {}", persona_id);

            let user_info = user_result.info.unwrap();

            let persona_info = db.get_persona_info_by_id(persona_id).await?;
            if persona_info.is_none() {
                // Insert
                db.upsert_persona_info(transaction, &PersonaInfo::new(persona_id, user_info.locality, user_info.location, user_info.presentation, user_info.login_counter, user_info.last_login)).await?;
            }
            else {
                // Update
                let mut persona_info = persona_info.unwrap();
                persona_info.locality = user_info.locality;
                persona_info.location = user_info.location;
                persona_info.presentation = user_info.presentation;
                persona_info.login_counter = user_info.login_counter;
                persona_info.last_login = user_info.last_login;
                db.update_persona_info_with_transaction(transaction, &persona_info).await?;
            }
        }
        else {
            trace!("Skipping updating persona info for {}", persona_id);
        }

        // Update persona last_updated time
        trace!("Updating last_update for {}", persona_id);

        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let time_now = since_the_epoch.as_secs();

        persona.last_updated = Some(time_now as u32);
        db.update_persona_with_transaction(transaction, &persona).await?;
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_update_non_premium_users() {
        update_non_premium_users()
            .await
            .unwrap();
    }
}

