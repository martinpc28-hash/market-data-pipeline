//! Polling de mercados de Polymarket para el tag "bitcoin" (y "crypto")
//! -- misma ruta rápida que tools/market_matcher.py::fetch_polymarket_markets_via_tags
//! (Python, ya probada contra la API real): /events?tag_slug=X en vez de
//! paginar todo /markets. Se repite cada POLL_INTERVAL.

use crate::state::{now_unix, PolyMarket, SharedStore};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

const POLYMARKET_EVENTS_URL: &str = "https://gamma-api.polymarket.com/events";

const POLL_INTERVAL: Duration = Duration::from_secs(3);
/// Tags a cubrir en esta primera versión "chica" -- ver charla con el
/// usuario: arrancamos solo con cripto/Bitcoin, no todas las categorías.
const TAG_SLUGS: &[&str] = &["bitcoin", "crypto"];

#[derive(Debug, Deserialize)]
struct EventEntry {
    markets: Option<Vec<MarketEntry>>,
}

#[derive(Debug, Deserialize)]
struct MarketEntry {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    question: Option<String>,
    outcomes: Option<Value>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<Value>,
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<Value>,
    #[serde(rename = "bestBid")]
    best_bid: Option<f64>,
    #[serde(rename = "bestAsk")]
    best_ask: Option<f64>,
    #[serde(rename = "volume24hr")]
    volume_24hr: Option<f64>,
    volume: Option<f64>,
}

/// Polymarket devuelve outcomes/clobTokenIds/outcomePrices a veces como
/// lista real, a veces como JSON-string -- mismo problema que
/// _normalize_poly_outcome_fields en Python, misma solución acá.
fn normalize_string_list(value: Option<Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .into_iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        Some(Value::String(s)) if !s.is_empty() => {
            serde_json::from_str::<Vec<String>>(&s).unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

async fn fetch_events_for_tag(
    client: &reqwest::Client,
    tag_slug: &str,
) -> anyhow::Result<Vec<PolyMarket>> {
    let resp = client
        .get(POLYMARKET_EVENTS_URL)
        .query(&[("tag_slug", tag_slug), ("closed", "false"), ("limit", "100")])
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?;
    let events: Vec<EventEntry> = resp.json().await?;
    let now = now_unix();

    let mut out = Vec::new();
    for event in events {
        for market in event.markets.unwrap_or_default() {
            let Some(condition_id) = market.condition_id else {
                continue;
            };
            out.push(PolyMarket {
                condition_id,
                title: market.question.unwrap_or_default(),
                outcomes: normalize_string_list(market.outcomes),
                token_ids: normalize_string_list(market.clob_token_ids),
                outcome_prices: normalize_string_list(market.outcome_prices),
                best_bid: market.best_bid,
                best_ask: market.best_ask,
                volume_24h: market.volume_24hr.or(market.volume),
                updated_at: now,
            });
        }
    }
    Ok(out)
}

pub async fn run(store: SharedStore) {
    let client = reqwest::Client::new();

    loop {
        let mut all_markets: Vec<PolyMarket> = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for tag in TAG_SLUGS {
            match fetch_events_for_tag(&client, tag).await {
                Ok(markets) => {
                    for m in markets {
                        // Un mismo mercado puede aparecer bajo más de un
                        // tag (ej. "bitcoin" y "crypto" a la vez) -- se
                        // deduplica por condition_id.
                        if seen.insert(m.condition_id.clone()) {
                            all_markets.push(m);
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(tag = %tag, error = %err, "polymarket: fallo al pedir /events para este tag, se sigue con los demás");
                }
            }
        }

        if !all_markets.is_empty() {
            tracing::debug!(count = all_markets.len(), "polymarket: snapshot actualizado");
            store.replace_polymarket(all_markets).await;
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_string_list_handles_real_array() {
        let v: Value = serde_json::json!(["Yes", "No"]);
        assert_eq!(normalize_string_list(Some(v)), vec!["Yes", "No"]);
    }

    #[test]
    fn normalize_string_list_handles_json_encoded_string() {
        // Confirmado en el resto del proyecto (_normalize_poly_outcome_fields
        // en Python): Polymarket a veces manda esto como string, no lista.
        let v: Value = serde_json::json!(r#"["Yes","No"]"#);
        assert_eq!(normalize_string_list(Some(v)), vec!["Yes", "No"]);
    }

    #[test]
    fn normalize_string_list_handles_missing() {
        assert_eq!(normalize_string_list(None), Vec::<String>::new());
    }

    #[test]
    fn parses_events_response_shape_matching_real_polymarket_payload() {
        let raw = r#"[
            {
                "markets": [
                    {
                        "conditionId": "0xabc123",
                        "question": "Will Bitcoin reach $150,000 by December 31, 2026?",
                        "outcomes": "[\"Yes\",\"No\"]",
                        "clobTokenIds": "[\"TOK_YES\",\"TOK_NO\"]",
                        "outcomePrices": "[\"0.20\",\"0.80\"]",
                        "bestBid": 0.19,
                        "bestAsk": 0.21,
                        "volume24hr": 1085552.79
                    }
                ]
            }
        ]"#;
        let events: Vec<EventEntry> = serde_json::from_str(raw).unwrap();
        let market = &events[0].markets.as_ref().unwrap()[0];
        assert_eq!(market.condition_id.as_deref(), Some("0xabc123"));
        assert_eq!(
            normalize_string_list(market.outcomes.clone()),
            vec!["Yes", "No"]
        );
        assert_eq!(
            normalize_string_list(market.clob_token_ids.clone()),
            vec!["TOK_YES", "TOK_NO"]
        );
        assert_eq!(market.best_bid, Some(0.19));
        assert_eq!(market.best_ask, Some(0.21));
    }

    #[test]
    fn parses_events_response_with_null_markets_without_panicking() {
        // Confirmado en el resto del proyecto: Polymarket a veces manda
        // "markets": null explícito, no ausente -- ver comentario sobre
        // esto en fetch_polymarket_markets_via_tags (Python).
        let raw = r#"[{"markets": null}]"#;
        let events: Vec<EventEntry> = serde_json::from_str(raw).unwrap();
        assert_eq!(events[0].markets.as_ref().map(Vec::len).unwrap_or(0), 0);
    }
}
