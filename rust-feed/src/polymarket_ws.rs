//! WebSocket en vivo de Polymarket -- canal "market", público, sin
//! credenciales (a diferencia de Kalshi). El polling REST (polymarket.rs)
//! sigue corriendo aparte para DESCUBRIR qué mercados existen (título,
//! condition_id, token_ids); este módulo solo actualiza el bid/ask del
//! lado "Yes" de los que el polling ya conoce (ver
//! Store::polymarket_token_index, que marca qué token_id es el Yes de
//! cada mercado).
//!
//! Documentación: wss://ws-subscriptions-clob.polymarket.com/ws/market,
//! sin auth, requiere mandar el texto "PING" cada 10s o el servidor
//! corta la conexión.

use crate::state::SharedStore;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_EVERY: Duration = Duration::from_secs(10);
/// Igual que en kalshi_ws.rs: se recicla la conexión de tanto en tanto
/// para re-suscribir con los token_ids que el polling REST haya
/// descubierto mientras tanto (mercados nuevos).
const RESUBSCRIBE_EVERY: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize)]
struct PriceChangeEvent {
    event_type: Option<String>,
    asset_id: Option<String>,
    // Forma alternativa: algunos mensajes traen los cambios en un array
    // "price_changes" en vez de en el nivel superior -- se soportan las
    // dos formas (ver docs.polymarket.com/market-data/websocket/market-channel).
    price_changes: Option<Vec<PriceChangeItem>>,
    #[serde(default, deserialize_with = "de_flexible_f64")]
    best_bid: Option<f64>,
    #[serde(default, deserialize_with = "de_flexible_f64")]
    best_ask: Option<f64>,
    // Evento "book": arrays de niveles, hay que sacar el mejor de cada
    // lado a mano (no viene precalculado como en price_change).
    bids: Option<Vec<BookLevel>>,
    asks: Option<Vec<BookLevel>>,
}

#[derive(Debug, Deserialize)]
struct PriceChangeItem {
    asset_id: Option<String>,
    #[serde(default, deserialize_with = "de_flexible_f64")]
    best_bid: Option<f64>,
    #[serde(default, deserialize_with = "de_flexible_f64")]
    best_ask: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    #[serde(deserialize_with = "de_flexible_f64_required")]
    price: f64,
}

fn de_flexible_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<Value> = serde::Deserialize::deserialize(deserializer)?;
    Ok(match value {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    })
}

fn de_flexible_f64_required<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Value = serde::Deserialize::deserialize(deserializer)?;
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| serde::de::Error::custom("número inválido")),
        Value::String(s) => s.parse::<f64>().map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom("se esperaba número o string")),
    }
}

/// Extrae (asset_id, best_bid, best_ask) de un mensaje, sea "book" o
/// "price_change" -- devuelve una lista porque "price_change" puede
/// traer varios asset_ids en un solo mensaje.
fn extract_updates(event: &PriceChangeEvent) -> Vec<(String, Option<f64>, Option<f64>)> {
    if let Some(changes) = &event.price_changes {
        return changes
            .iter()
            .filter_map(|c| {
                let asset_id = c.asset_id.clone()?;
                Some((asset_id, c.best_bid, c.best_ask))
            })
            .collect();
    }
    if let Some(asset_id) = &event.asset_id {
        if event.best_bid.is_some() || event.best_ask.is_some() {
            return vec![(asset_id.clone(), event.best_bid, event.best_ask)];
        }
        // Evento "book" sin best_bid/best_ask directo -- se calcula del
        // libro: mejor bid = precio más alto ofertado para comprar,
        // mejor ask = precio más bajo ofrecido para vender.
        let best_bid = event
            .bids
            .as_ref()
            .and_then(|levels| levels.iter().map(|l| l.price).fold(None, |acc: Option<f64>, p| {
                Some(acc.map_or(p, |a| a.max(p)))
            }));
        let best_ask = event
            .asks
            .as_ref()
            .and_then(|levels| levels.iter().map(|l| l.price).fold(None, |acc: Option<f64>, p| {
                Some(acc.map_or(p, |a| a.min(p)))
            }));
        if best_bid.is_some() || best_ask.is_some() {
            return vec![(asset_id.clone(), best_bid, best_ask)];
        }
    }
    Vec::new()
}

async fn run_one_connection(store: &SharedStore) -> anyhow::Result<()> {
    // Solo interesa el lado "Yes" de cada mercado (misma convención que
    // _implied_yes_price_polymarket en Python) -- el lado "No" se sigue
    // resolviendo aparte cuando hace falta (ver _poly_token_ask en
    // tools/market_matcher.py, CLOB REST puntual solo para candidatos de
    // arbitraje ya calificados).
    let index = store.polymarket_token_index().await;
    let yes_tokens: Vec<String> = index
        .into_iter()
        .filter(|(_, _, is_yes)| *is_yes)
        .map(|(token_id, _, _)| token_id)
        .collect();
    if yes_tokens.is_empty() {
        anyhow::bail!("sin tokens descubiertos todavía (el polling REST no completó su primer ciclo)");
    }

    tracing::info!(tokens = yes_tokens.len(), "polymarket ws: conectando");
    let (ws_stream, _resp) = tokio_tungstenite::connect_async(WS_URL).await?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe_msg = serde_json::json!({
        "assets_ids": yes_tokens,
        "type": "market",
    });
    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await?;
    tracing::info!("polymarket ws: suscripto, esperando mensajes");

    let deadline = tokio::time::sleep(RESUBSCRIBE_EVERY);
    tokio::pin!(deadline);
    let mut ping_interval = tokio::time::interval(PING_EVERY);

    // El índice token_id -> condition_id se recalcula UNA vez por
    // conexión (no en cada mensaje) -- suficiente para la duración de
    // esta conexión, se refresca solo al reconectar.
    let token_to_condition: std::collections::HashMap<String, String> = store
        .polymarket_token_index()
        .await
        .into_iter()
        .map(|(token, condition, _)| (token, condition))
        .collect();

    loop {
        tokio::select! {
            _ = &mut deadline => {
                tracing::debug!("polymarket ws: ciclo de re-suscripción, reconectando");
                return Ok(());
            }
            _ = ping_interval.tick() => {
                if write.send(Message::Text("PING".to_string())).await.is_err() {
                    anyhow::bail!("polymarket ws: no se pudo mandar PING, la conexión parece caída");
                }
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("polymarket ws: conexión cerrada por el servidor");
                };
                let msg = msg?;
                let text = match msg {
                    Message::Text(t) => t,
                    Message::Ping(_) | Message::Pong(_) => continue,
                    _ => continue,
                };
                if text == "PONG" {
                    continue;
                }
                let Ok(event) = serde_json::from_str::<PriceChangeEvent>(&text) else { continue };
                if !matches!(event.event_type.as_deref(), Some("book") | Some("price_change") | None) {
                    continue;
                }
                for (asset_id, best_bid, best_ask) in extract_updates(&event) {
                    let Some(condition_id) = token_to_condition.get(&asset_id) else { continue };
                    store
                        .update_polymarket_yes_price(condition_id, best_bid, best_ask)
                        .await;
                }
            }
        }
    }
}

pub async fn run(store: SharedStore) {
    loop {
        match run_one_connection(&store).await {
            Ok(()) => {} // ciclo de re-suscripción normal, reconectar ya
            Err(err) => {
                tracing::warn!(error = %err, "polymarket ws: se cortó la conexión, reintentando en 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_updates_from_price_change_array_shape() {
        let raw = r#"{
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "TOK_YES", "best_bid": "0.19", "best_ask": "0.21"}
            ]
        }"#;
        let event: PriceChangeEvent = serde_json::from_str(raw).unwrap();
        let updates = extract_updates(&event);
        assert_eq!(updates, vec![("TOK_YES".to_string(), Some(0.19), Some(0.21))]);
    }

    #[test]
    fn extracts_updates_from_top_level_asset_shape() {
        let raw = r#"{"event_type":"price_change","asset_id":"TOK_YES","best_bid":0.19,"best_ask":0.21}"#;
        let event: PriceChangeEvent = serde_json::from_str(raw).unwrap();
        let updates = extract_updates(&event);
        assert_eq!(updates, vec![("TOK_YES".to_string(), Some(0.19), Some(0.21))]);
    }

    #[test]
    fn extracts_best_bid_ask_from_book_levels() {
        let raw = r#"{
            "event_type": "book",
            "asset_id": "TOK_YES",
            "bids": [{"price": "0.18"}, {"price": "0.19"}],
            "asks": [{"price": "0.22"}, {"price": "0.21"}]
        }"#;
        let event: PriceChangeEvent = serde_json::from_str(raw).unwrap();
        let updates = extract_updates(&event);
        // mejor bid = el más alto (0.19), mejor ask = el más bajo (0.21)
        assert_eq!(updates, vec![("TOK_YES".to_string(), Some(0.19), Some(0.21))]);
    }

    #[test]
    fn returns_empty_for_message_with_no_usable_prices() {
        let raw = r#"{"event_type":"price_change","asset_id":"TOK_YES"}"#;
        let event: PriceChangeEvent = serde_json::from_str(raw).unwrap();
        assert!(extract_updates(&event).is_empty());
    }
}
