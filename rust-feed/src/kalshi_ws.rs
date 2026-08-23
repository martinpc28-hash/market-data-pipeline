//! WebSocket en vivo de Kalshi -- canal "ticker" (precio actual, no hace
//! falta reconstruir el order book completo a partir de deltas). El
//! polling REST (kalshi.rs) sigue corriendo aparte para DESCUBRIR qué
//! mercados existen (título, ticker, volumen); este módulo solo
//! actualiza el bid/ask de los que el polling ya conoce, en cuanto
//! Kalshi los publica -- mucho más rápido que esperar el próximo ciclo
//! de polling.
//!
//! Requiere las credenciales de Kalshi (KALSHI_API_KEY_ID +
//! KALSHI_PRIVATE_KEY_PATH, ver kalshi_auth.rs) -- si no están
//! configuradas, este módulo simplemente no arranca (se loguea una vez
//! y listo) y el servicio sigue funcionando solo con polling REST, como
//! antes -- nunca es un requisito duro para que crypto-live-feed sirva
//! datos.

use crate::kalshi_auth::KalshiCredentials;
use crate::state::SharedStore;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

/// Mismo mapeo host/path que ingestion/kalshi_ingest.py (WS_PATHS) --
/// KALSHI_ENV decide cuál, mismo criterio que el resto del proyecto.
fn ws_host_and_path(env_name: &str) -> Option<(&'static str, &'static str)> {
    match env_name {
        "prod" => Some(("external-api-ws.kalshi.com", "/trade-api/ws/v2")),
        "demo" => Some(("external-api-ws.demo.kalshi.co", "/trade-api/ws/v2")),
        _ => None,
    }
}

/// Cada cuánto se recicla la conexión para re-suscribir con la lista
/// actualizada de tickers (el polling REST puede haber descubierto
/// mercados nuevos mientras tanto) -- además de reconectar ante
/// cualquier error, claro.
const RESUBSCRIBE_EVERY: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize)]
struct TickerEnvelope {
    #[serde(rename = "type")]
    kind: Option<String>,
    msg: Option<TickerMsg>,
}

#[derive(Debug, Deserialize)]
struct TickerMsg {
    market_ticker: Option<String>,
    // Kalshi manda estos campos en dólares (ej. "0.42"), no en centavos
    // como el endpoint REST /markets -- hay que convertir. A veces
    // vienen como número, a veces como string según el mensaje -- se
    // acepta cualquiera de las dos formas.
    #[serde(default, deserialize_with = "de_flexible_f64")]
    yes_bid_dollars: Option<f64>,
    #[serde(default, deserialize_with = "de_flexible_f64")]
    yes_ask_dollars: Option<f64>,
}

fn de_flexible_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<serde_json::Value> = serde::Deserialize::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    })
}

async fn run_one_connection(
    store: &SharedStore,
    creds: &KalshiCredentials,
    host: &str,
    path: &str,
) -> anyhow::Result<()> {
    let tickers = store.kalshi_tickers().await;
    if tickers.is_empty() {
        // Todavía no hay nada que el polling REST haya descubierto --
        // no vale la pena conectar sin saber a qué suscribirse.
        anyhow::bail!("sin tickers descubiertos todavía (el polling REST no completó su primer ciclo)");
    }

    let url = format!("wss://{host}{path}");
    let mut request = url.clone().into_client_request()?;
    for (name, value) in creds.auth_headers("GET", path) {
        request.headers_mut().insert(
            http::HeaderName::try_from(name.as_str())?,
            http::HeaderValue::from_str(&value)?,
        );
    }

    tracing::info!(url = %url, tickers = tickers.len(), "kalshi ws: conectando");
    let (ws_stream, _resp) = tokio_tungstenite::connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe_msg = serde_json::json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["ticker"],
            "market_tickers": tickers,
        },
    });
    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await?;
    tracing::info!("kalshi ws: suscripto, esperando mensajes");

    let deadline = tokio::time::sleep(RESUBSCRIBE_EVERY);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => {
                tracing::debug!("kalshi ws: ciclo de re-suscripción, reconectando");
                return Ok(());
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("kalshi ws: conexión cerrada por el servidor");
                };
                let msg = msg?;
                let Message::Text(text) = msg else { continue };
                let Ok(envelope) = serde_json::from_str::<TickerEnvelope>(&text) else { continue };
                if envelope.kind.as_deref() != Some("ticker") {
                    if envelope.kind.as_deref() == Some("error") {
                        tracing::warn!(msg = %text, "kalshi ws: mensaje de error del servidor");
                    }
                    continue;
                }
                let Some(m) = envelope.msg else { continue };
                let Some(ticker) = m.market_ticker else { continue };
                // Kalshi manda dólares acá, el resto del sistema (REST,
                // Python) trabaja en centavos -- se convierte al entrar.
                let yes_bid = m.yes_bid_dollars.map(|v| v * 100.0);
                let yes_ask = m.yes_ask_dollars.map(|v| v * 100.0);
                store.update_kalshi_price(&ticker, yes_bid, yes_ask).await;
            }
        }
    }
}

pub async fn run(store: SharedStore) {
    let creds = match KalshiCredentials::from_env() {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "kalshi ws: sin credenciales configuradas, se sigue solo con polling REST (no es un error fatal)"
            );
            return;
        }
    };

    let env_name = std::env::var("KALSHI_ENV")
        .unwrap_or_else(|_| "demo".to_string())
        .trim()
        .to_lowercase();
    let Some((host, path)) = ws_host_and_path(&env_name) else {
        tracing::warn!(env = %env_name, "kalshi ws: KALSHI_ENV inválido (usar 'demo' o 'prod'), no se conecta por WS");
        return;
    };

    loop {
        match run_one_connection(&store, &creds, host, path).await {
            Ok(()) => {} // ciclo de re-suscripción normal, reconectar ya
            Err(err) => {
                tracing::warn!(error = %err, "kalshi ws: se cortó la conexión, reintentando en 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ticker_message_with_string_prices() {
        let raw = r#"{
            "type": "ticker",
            "msg": {
                "market_ticker": "KXBTCMAXY-26DEC31-149999.99",
                "yes_bid_dollars": "0.68",
                "yes_ask_dollars": "0.70"
            }
        }"#;
        let envelope: TickerEnvelope = serde_json::from_str(raw).unwrap();
        assert_eq!(envelope.kind.as_deref(), Some("ticker"));
        let msg = envelope.msg.unwrap();
        assert_eq!(msg.market_ticker.as_deref(), Some("KXBTCMAXY-26DEC31-149999.99"));
        assert_eq!(msg.yes_bid_dollars, Some(0.68));
        assert_eq!(msg.yes_ask_dollars, Some(0.70));
    }

    #[test]
    fn parses_ticker_message_with_numeric_prices() {
        let raw = r#"{"type":"ticker","msg":{"market_ticker":"T","yes_bid_dollars":0.5,"yes_ask_dollars":0.55}}"#;
        let envelope: TickerEnvelope = serde_json::from_str(raw).unwrap();
        let msg = envelope.msg.unwrap();
        assert_eq!(msg.yes_bid_dollars, Some(0.5));
        assert_eq!(msg.yes_ask_dollars, Some(0.55));
    }

    #[test]
    fn ignores_non_ticker_envelope_types_without_panicking() {
        let raw = r#"{"type":"subscribed","msg":{}}"#;
        let envelope: TickerEnvelope = serde_json::from_str(raw).unwrap();
        assert_eq!(envelope.kind.as_deref(), Some("subscribed"));
    }

    #[test]
    fn ws_host_and_path_covers_demo_and_prod() {
        assert_eq!(
            ws_host_and_path("prod"),
            Some(("external-api-ws.kalshi.com", "/trade-api/ws/v2"))
        );
        assert_eq!(
            ws_host_and_path("demo"),
            Some(("external-api-ws.demo.kalshi.co", "/trade-api/ws/v2"))
        );
        assert_eq!(ws_host_and_path("bogus"), None);
    }
}
