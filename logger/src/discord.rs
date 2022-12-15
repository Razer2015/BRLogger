use chrono::{Utc};
use webhook::client::{WebhookClient, WebhookResult};

pub async fn send_message_webhook(webhook_url: &str, soldier_name: &str, chat_message: &str) -> WebhookResult<()> {
    let message_time = Utc::now();

    let client: WebhookClient = WebhookClient::new(webhook_url);
    let webhook_info = client.get_information().await?;
    debug!("webhook: {:?}", webhook_info);

    client.send(|message| message
        .username(soldier_name)
        .avatar_url("https://eaassets-a.akamaihd.net/battlelog/defaultavatars/default-avatar-36.png")
        .embed(|embed| embed
            .description(chat_message)
            .footer("bit.ly/bf4chat by xfileFIN", None)
            .timestamp(&message_time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
            .color("15790320")
        )
    ).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;

    #[tokio::test]
    async fn test_webhook_post() {
        dotenv().ok();

        let webhook_path = dotenv::var("DISCORD_WEBHOOK").unwrap();

        send_message_webhook(&webhook_path, "Webhook tester", "Test message").await.unwrap();
    }
}
