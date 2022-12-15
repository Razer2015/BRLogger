use battlefield_rcon::bf4::player_info_block::PlayerInfo;
use battlelog::get_users;

pub async fn get_round_over_data(players: Vec<PlayerInfo>) {
    info!("Retrieving round over data with {} players", players.len());

    let soldier_names: Vec<String> = players.iter().map(|p| p.player_name.to_string()).collect();
    let users = get_users(soldier_names).await.unwrap();

    for user in &users {
        trace!("Checking BattleReports for user {}", user.persona.persona_name);

        
    }
}