use actix_web::{get, web, HttpResponse, Responder};
use serde::{Deserialize};

use crate::loadout::loadout_checker::get_user_loadout;


#[derive(Deserialize)]
pub struct LoadoutParams {
    soldier_name: String,
    persona_id: String,
}

#[get("/loadout/{soldier_name}/{persona_id}")]
pub async fn get_persona_loadout(params: web::Path<LoadoutParams>) -> impl Responder {
    
    match get_user_loadout(&params.soldier_name, &params.persona_id).await {
        Ok(report) => {
            return HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .json(report)
        },
        Err(err) => {
            return HttpResponse::InternalServerError().body(format!("Error {:?}", err))
        },
    };
}
