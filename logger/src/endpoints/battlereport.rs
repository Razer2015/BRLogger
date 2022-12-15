use actix_web::{get, post, web, HttpResponse, Responder};
use battlelog::{battlereport, BattlereportResponse, playerreport, PlayerreportResponse};
use serde::{Deserialize, Serialize};

use crate::{round_stats::battlereport::{add_battlereport_by_id, fetch_battlereports_for_user}, database::battlelog::battlereports::BattlelogBattlereport};

#[get("/battlereport/{report_id}")]
pub async fn get_battlereport_by_id(report_id: web::Path<String>) -> impl Responder {
    
    match get_battlereport(&report_id).await {
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

#[derive(Deserialize)]
pub struct PlayerReportParams {
    report_id: String,
    persona_id: String,
}

#[get("/battlereport/{report_id}/{persona_id}")]
pub async fn get_playerreport_by_id(params: web::Path<PlayerReportParams>) -> impl Responder {
    
    match get_playerreport(&params.report_id, &params.persona_id).await {
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

#[derive(Deserialize)]
pub struct BattlereportsMoreParams {
    persona_id: String,
    timestamp: Option<String>,
}

#[get("/battlereports/{persona_id}/{timestamp}")]
pub async fn get_battlereports_more(params: web::Path<BattlereportsMoreParams>) -> impl Responder {
    match fetch_battlereports_for_user(&params.persona_id, &params.timestamp).await {
        Ok(data) => {
            return HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .json(data)
        },
        Err(err) => {
            return HttpResponse::InternalServerError().body(format!("Error {:?}", err))
        },
    };
}

#[get("/battlereports/text/{persona_id}/{timestamp}")]
pub async fn get_battlereports_more_text(params: web::Path<BattlereportsMoreParams>) -> impl Responder {
    match fetch_battlereports_for_user(&params.persona_id, &params.timestamp).await {
        Ok(data) => {
            let test: Vec<String> = data.iter().map(|r| format!("{} $ {}", r.game_report_id, r.name)).collect();

            return HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .json(test)
        },
        Err(err) => {
            return HttpResponse::InternalServerError().body(format!("Error {:?}", err))
        },
    };
}

#[derive(Debug, Serialize)]
pub struct BattleReportAddingResponse {
    pub success: bool,
    pub report: BattlelogBattlereport,
    pub errors: Option<Vec<String>>
}

#[post("/battlereport/{report_id}")]
pub async fn post_battlereport_by_id(report_id: web::Path<String>) -> impl Responder {
    match add_battlereport_by_id(&report_id).await {
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

async fn get_battlereport(report_id: &str) -> Result<BattlereportResponse, anyhow::Error> {
    let report = battlereport(report_id).await?;
    Ok(report)
}

async fn get_playerreport(report_id: &str, persona_id: &str) -> Result<PlayerreportResponse, anyhow::Error> {
    let report = playerreport(report_id, persona_id).await?;
    Ok(report)
}