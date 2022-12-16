#[macro_use]
extern crate log;

use actix_web::{middleware, web, App, HttpServer};
use battlefield_rcon::{rcon::{RconConnectionInfo}, bf4::{Bf4Client, Event, Visibility}};
use chrono_tz::Tz;
use dotenv::{dotenv, var};
use round_stats::battlereport;
use ascii::{IntoAsciiString};
use async_std::task;
use futures::join;
use futures::StreamExt;

use crate::discord::send_message_webhook;

mod database;
mod discord;
mod logging;
mod round_stats;
mod endpoints;
pub mod loadout;
mod persona;

fn get_rcon_coninfo() -> anyhow::Result<RconConnectionInfo> {
    let ip = var("RCON_IP").unwrap_or_else(|_| "127.0.0.1".into());
    let port = var("RCON_PORT")
        .unwrap_or_else(|_| "47200".into())
        .parse::<u16>()
        .unwrap();
    let password = var("RCON_PASSWORD").unwrap_or_else(|_| "smurf".into());
    Ok(RconConnectionInfo {
        ip,
        port,
        password: password.into_ascii_string()?,
    })
}

fn get_timezone() -> Tz {
    let timezone = dotenv::var("CHRONO_TIMEZONE").unwrap_or("Europe/Helsinki".to_string());
    timezone.parse().unwrap()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    logging::init_logging();

    info!("BR Logger starting");
    info!("Using time zone: {}", get_timezone().name());

    if dotenv::var("UPDATE_PERSONAS").map(|var| var.parse::<bool>()).unwrap_or(Ok(false)).unwrap() {
        persona::persona_updater::update_personas_without_last_update().await?;
    }
    
    if dotenv::var("READ_BRR").map(|var| var.parse::<bool>()).unwrap_or(Ok(false)).unwrap() {
        battlereport::read_brr(&dotenv::var("BRR_PATH").unwrap()).await?;
    }

    if dotenv::var("READ_REPORT_IDS").map(|var| var.parse::<bool>()).unwrap_or(Ok(false)).unwrap() {
        battlereport::read_battlereport_ids(&dotenv::var("REPORT_IDS_PATH").unwrap()).await?;
    }

    // let webhook_path = dotenv::var("DISCORD_WEBHOOK").unwrap();
    let rconinfo = get_rcon_coninfo()?;

    let bf4 = Bf4Client::connect((rconinfo.ip, rconinfo.port), rconinfo.password)
        .await
        .unwrap();

    let events_bf4 = bf4.clone();
    let events_task = task::spawn(async move {
        if dotenv::var("ENABLE_RCON_EVENTS").map(|var| var.parse::<bool>()).unwrap_or(Ok(true)).unwrap() {
            let mut event_stream = events_bf4.event_stream().await.unwrap();
            while let Some(ev) = event_stream.next().await {
                match ev {
                    Ok(Event::RoundOverPlayers { players }) => {

                    },
                    Ok(_) => {}, // ignore other events.
                    Err(err) => {
                        error!("Got error: {:?}", err);
                    },
                }
            }
        }
    });

    let rest_api_address = dotenv::var("RESTAPI_ADDRESS").unwrap_or("0.0.0.0".to_string());
    let rest_api_port: u16 = dotenv::var("RESTAPI_PORT")
        .map(|var| var.parse::<u16>())
        .unwrap_or(Ok(8080))
        .unwrap();

    info!(
        "Binding REST API to http://{}:{}",
        &rest_api_address, &rest_api_port
    );

    let rest_api = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Compress::default())
            .app_data(web::Data::new(bf4.clone()))
            .service(endpoints::health::check)
            .service(endpoints::battlereport::get_battlereport_by_id)
            .service(endpoints::battlereport::get_playerreport_by_id)
            .service(endpoints::battlereport::post_battlereport_by_id)
            .service(endpoints::battlereport::get_battlereports_more)
            .service(endpoints::battlereport::get_battlereports_more_text)
            .service(endpoints::loadout::get_persona_loadout)
    })
    .bind((rest_api_address, rest_api_port))
    .unwrap()
    .run();

    _ = join!(events_task, rest_api);

    Ok(())
}
