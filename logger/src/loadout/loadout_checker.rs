use battlelog::{get_loadout, LoadoutResult};

pub async fn get_user_loadout(soldier_name: &str, persona_id: &str) -> anyhow::Result<LoadoutResult> {
    Ok(get_loadout(soldier_name, persona_id).await?)
}
