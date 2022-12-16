use battlelog::{PlayerreportResponse};
use serde::Serialize;
use sqlx::{query_as, query, MySql, Transaction};

use super::context::BattlelogContext;

#[derive(Debug, Serialize)]
pub struct BattlelogPersona {
    pub id: u64,
    pub name: Option<String>,
    pub clan_tag: Option<String>,
    pub gravatar_md5: Option<String>,
    pub processed: bool,
    pub last_updated: Option<u32>,
}

impl BattlelogPersona {
    pub fn new(id: u64, name: Option<String>, clan_tag: Option<String>, gravatar_md5: Option<String>, processed: bool, last_updated: Option<u32>) -> Self {
        Self { 
            id: id,
            name,
            clan_tag,
            gravatar_md5,
            processed,
            last_updated,
        }
    }

    pub fn from_playerreport_response(response: &PlayerreportResponse) -> Self {
        BattlelogPersona::from_playerreport_response_with_gravatar(response, None)
    }

    pub fn from_playerreport_response_with_gravatar(response: &PlayerreportResponse, gravatar: Option<String>) -> Self {
        let persona_id = response.persona_id.parse::<u64>().unwrap();
        let persona = response.persona.as_ref();
        
        if persona.is_some() {
            let persona = persona.unwrap();
            Self {
                id: persona_id,
                name: Some(persona.persona_name.clone()),
                clan_tag: persona.clan_tag.clone(),
                gravatar_md5: gravatar,
                processed: true,
                last_updated: None,
            }
        }
        else {
            Self {
                id: persona_id,
                name: None,
                clan_tag: None,
                gravatar_md5: None,
                processed: false,
                last_updated: None,
            }
        }
    }
}

impl BattlelogContext {
    pub async fn get_persona_by_persona_id(&self, persona: u64) -> Result<Option<BattlelogPersona>, sqlx::Error> {
        struct Row {
            pub id: u64,
            pub name: Option<String>,
            pub clan_tag: Option<String>,
            pub gravatar_md5: Option<String>,
            pub processed: u8,
            pub last_updated: Option<u32>,
        }

        let res =
            query_as!(Row, "SELECT * from personas WHERE id = ?", persona)
            .fetch_optional(&self.pool)
            .await?;

        let res = res.map(|e: Row| BattlelogPersona {
            id: e.id,
            name: e.name,
            clan_tag: e.clan_tag,
            gravatar_md5: e.gravatar_md5,
            processed: e.processed == 1,
            last_updated: e.last_updated,
        });

        Ok(res)
    }

    pub async fn get_persona_by_persona_id_str(&self, persona: &str) -> Result<Option<BattlelogPersona>, sqlx::Error> {
        pub struct Row {
            pub id: u64,
            pub name: Option<String>,
            pub clan_tag: Option<String>,
            pub gravatar_md5: Option<String>,
            pub processed: u8,
            pub last_updated: Option<u32>,
        }

        let res =
            query_as!(Row, "SELECT * from personas WHERE id = ?", persona)
            .fetch_optional(&self.pool)
            .await?;

        let res = res.map(|e: Row| BattlelogPersona {
            id: e.id,
            name: e.name,
            clan_tag: e.clan_tag,
            gravatar_md5: e.gravatar_md5,
            processed: e.processed == 1,
            last_updated: e.last_updated,
        });

        Ok(res)
    }

    pub async fn get_personas_without_update(&self) -> Result<Vec<BattlelogPersona>, sqlx::Error> {
        pub struct Row {
            pub id: u64,
            pub name: Option<String>,
            pub clan_tag: Option<String>,
            pub gravatar_md5: Option<String>,
            pub processed: u8,
            pub last_updated: Option<u32>,
        }

        let mut res: Vec<Row> =
            query_as!(Row, "SELECT * from personas WHERE last_updated IS NULL")
            .fetch_all(&self.pool)
            .await?;

        let res: Vec<BattlelogPersona> = res.drain(..).map(|e: Row| BattlelogPersona {
            id: e.id,
            name: e.name,
            clan_tag: e.clan_tag,
            gravatar_md5: e.gravatar_md5,
            processed: e.processed == 1,
            last_updated: e.last_updated,
        }).collect();

        Ok(res)
    }

    pub async fn insert_persona(&self, persona: &BattlelogPersona) -> anyhow::Result<u64> {
        self.insert_persona_private(None, persona).await
    }

    pub async fn insert_persona_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, persona: &BattlelogPersona) -> anyhow::Result<u64> {
        self.insert_persona_private(Some(transaction), persona).await
    }

    async fn insert_persona_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, persona: &BattlelogPersona) -> anyhow::Result<u64> {
        let mut res = 0;

        let query = query!(r#"INSERT INTO personas (id, name, clan_tag, gravatar_md5, processed) VALUES (?, ?, ?, ?, ?)"#, persona.id, persona.name, persona.clan_tag, persona.gravatar_md5, persona.processed);
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

    pub async fn upsert_persona(&self, transaction: &mut Transaction<'_, MySql>, persona: &BattlelogPersona) -> anyhow::Result<u64> {
        let query = query!(r#"INSERT IGNORE INTO personas (id, name, clan_tag, gravatar_md5, processed) VALUES (?, ?, ?, ?, ?)"#, persona.id, persona.name, persona.clan_tag, persona.gravatar_md5, persona.processed);
        let res = query
            .execute(&mut *transaction)
            .await?
            .last_insert_id();

        Ok(res)
    }

    pub async fn update_persona(&self, persona: &BattlelogPersona) -> anyhow::Result<bool> {
        self.update_persona_private(None, persona).await
    }

    pub async fn update_persona_with_transaction(&self, transaction: &mut Transaction<'_, MySql>, persona: &BattlelogPersona) -> anyhow::Result<bool> {
        self.update_persona_private(Some(transaction), persona).await
    }

    async fn update_persona_private(&self, transaction: Option<&mut Transaction<'_, MySql>>, persona: &BattlelogPersona) -> anyhow::Result<bool> {
        let mut res: u64 = 0;

        let query = query!(r#"UPDATE personas SET name = ?, clan_tag = ?, gravatar_md5 = ?, processed = ?, last_updated = ? WHERE id = ?"#, 
            persona.name, 
            persona.clan_tag, 
            persona.gravatar_md5, 
            persona.processed, 
            persona.last_updated, 
            persona.id);
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