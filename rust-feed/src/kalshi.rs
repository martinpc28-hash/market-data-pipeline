//! Polling de mercados de Kalshi para las series de cripto -- misma
//! lógica de dos pasos que tools/market_matcher.py::fetch_kalshi_markets_via_series
//! (Python, ya probado contra la API real): primero /series?category=Crypto
//! para conseguir el catálogo chico de series, después /markets?series_ticker=X
//! por cada serie encontrada. Acá se repite cada POLL_INTERVAL en vez de
//! una sola vez por búsqueda -- por eso es "en vivo" y no bajo demanda.

use crate::state::{now_unix, KalshiMarket, SharedStore};
use serde::Deserialize;
use std::time::Duration;

const KALSHI_SERIES_URL: &str = "https://api.elections.kalshi.com/trade-api/v2/series";
const KALSHI_MARKETS_URL: &str = "https://api.elections.kalshi.com/trade-api/v2/markets";

/// Cada cuánto se repite el ciclo completo (series + markets). 3s es
/// "en vivo" para efectos prácticos de un dashboard, sin ser agresivo
/// contra la API pública de Kalshi (evita esto convertirse en un
/// mini-DDoS si algún día se agregan más categorías).
const POLL_INTERVAL: Duration = Duration::from_secs(3);
/// El catálogo de series por categoría cambia poco (series nuevas se
/// abren cada tanto, no cada 3s) -- no hace falta repetir ESTA llamada
/// tan seguido, solo la de /markets (que sí tiene bid/ask en vivo).
const SERIES_REFRESH_EVERY: u32 = 200; // 200 * 3s ≈ 10 min

#[derive(Debug, Deserialize)]
struct SeriesListResponse {
    series: Option<Vec<SeriesEntry>>,
}

#[derive(Debug, Deserialize)]
struct SeriesEntry {
    ticker: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MarketsListResponse {
    markets: Option<Vec<MarketEntry>>,
}

#[derive(Debug, Deserialize)]
struct MarketEntry {
    ticker: Option<String>,
    event_ticker: Option<String>,
    title: Option<String>,
    yes_sub_title: Option<String>,
    yes_bid: Option<f64>,
    yes_ask: Option<f64>,
    volume_24h: Option<f64>,
    volume: Option<f64>,
}

/// Mismo criterio que _kalshi_display_title en Python: el título de
/// evento se repite entre mercados de un mismo evento (ej. "quién gana
/// la elección"), yes_sub_title es lo que distingue cada mercado
/// puntual -- se combinan para que no queden títulos duplicados/vacíos
/// de contexto en el feed.
fn display_title(title: Option<&str>, sub: Option<&str>) -> String {
    let title = title.unwrap_or("");
    match sub {
        Some(s) if !s.is_empty() && !title.contains(s) => {
            if title.is_empty() {
                s.to_string()
            } else {
                format!("{title} — {s}")
            }
        }
        _ => title.to_string(),
    }
}

async fn fetch_crypto_series(client: &reqwest::Client) -> anyhow::Result<Vec<String>> {
    let resp = client
        .get(KALSHI_SERIES_URL)
        .query(&[("category", "Crypto")])
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?;
    let body: SeriesListResponse = resp.json().await?;
    Ok(body
        .series
        .unwrap_or_default()
        .into_iter()
        .filter_map(|s| s.ticker)
        .collect())
}

async fn fetch_markets_for_series(
    client: &reqwest::Client,
    series_ticker: &str,
) -> anyhow::Result<Vec<KalshiMarket>> {
    let resp = client
        .get(KALSHI_MARKETS_URL)
        .query(&[
            ("series_ticker", series_ticker),
            ("status", "open"),
            ("limit", "200"),
        ])
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?;
    let body: MarketsListResponse = resp.json().await?;
    let now = now_unix();
    Ok(body
        .markets
        .unwrap_or_default()
        .into_iter()
        .filter_map(|m| {
            let ticker = m.ticker?;
            Some(KalshiMarket {
                ticker,
                event_ticker: m.event_ticker,
                title: display_title(m.title.as_deref(), m.yes_sub_title.as_deref()),
                yes_bid: m.yes_bid,
                yes_ask: m.yes_ask,
                volume_24h: m.volume_24h.or(m.volume),
                updated_at: now,
            })
        })
        .collect())
}

/// Loop de fondo -- nunca termina (salvo panic), reintenta solo ante
/// cualquier error de red sin tirar abajo el proceso: un error puntual
/// de la API de Kalshi no debe matar el servicio entero, solo se loguea
/// y se reintenta en el próximo ciclo.
pub async fn run(store: SharedStore) {
    let client = reqwest::Client::new();
    let mut cycle: u32 = 0;
    let mut series_cache: Vec<String> = Vec::new();

    loop {
        if series_cache.is_empty() || cycle % SERIES_REFRESH_EVERY == 0 {
            match fetch_crypto_series(&client).await {
                Ok(series) => {
                    tracing::info!(count = series.len(), "kalshi: series de cripto actualizadas");
                    series_cache = series;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "kalshi: no se pudo refrescar /series?category=Crypto, se sigue con el catálogo anterior");
                }
            }
        }

        if !series_cache.is_empty() {
            let mut all_markets = Vec::new();
            for ticker in &series_cache {
                match fetch_markets_for_series(&client, ticker).await {
                    Ok(markets) => all_markets.extend(markets),
                    Err(err) => {
                        tracing::warn!(series = %ticker, error = %err, "kalshi: fallo al pedir /markets de esta serie, se sigue con las demás");
                    }
                }
            }
            tracing::debug!(count = all_markets.len(), "kalshi: snapshot actualizado");
            store.replace_kalshi(all_markets).await;
        }

        cycle = cycle.wrapping_add(1);
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_title_combines_event_and_market_titles() {
        assert_eq!(
            display_title(Some("Will Bitcoin hit $150k?"), Some("$150,000")),
            "Will Bitcoin hit $150k? — $150,000"
        );
    }

    #[test]
    fn display_title_skips_sub_when_already_in_title() {
        assert_eq!(
            display_title(Some("Above $150,000?"), Some("$150,000")),
            "Above $150,000?"
        );
    }

    #[test]
    fn display_title_falls_back_to_sub_when_title_missing() {
        assert_eq!(display_title(None, Some("$150,000")), "$150,000");
    }

    #[test]
    fn parses_series_list_response() {
        let raw = r#"{"series": [{"ticker": "KXBTCMAXY"}, {"ticker": "KXBTCMAX150"}]}"#;
        let body: SeriesListResponse = serde_json::from_str(raw).unwrap();
        let tickers: Vec<_> = body
            .series
            .unwrap()
            .into_iter()
            .filter_map(|s| s.ticker)
            .collect();
        assert_eq!(tickers, vec!["KXBTCMAXY", "KXBTCMAX150"]);
    }

    #[test]
    fn parses_markets_list_response_shape_matching_real_kalshi_payload() {
        // Forma real confirmada en el resto del proyecto (ver
        // tools/market_matcher.py, _kalshi_display_title y _kalshi_cents).
        let raw = r#"{
            "markets": [
                {
                    "ticker": "KXBTCMAXY-26DEC31-99999.99",
                    "event_ticker": "KXBTCMAXY-26DEC31",
                    "title": "Will Bitcoin be above $99,999.99 by Dec 31, 2026?",
                    "yes_sub_title": "Above $99,999.99",
                    "yes_bid": 4,
                    "yes_ask": 6,
                    "volume_24h": 14970.81
                }
            ]
        }"#;
        let body: MarketsListResponse = serde_json::from_str(raw).unwrap();
        let markets = body.markets.unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].ticker.as_deref(), Some("KXBTCMAXY-26DEC31-99999.99"));
        assert_eq!(markets[0].yes_bid, Some(4.0));
        assert_eq!(markets[0].yes_ask, Some(6.0));
    }
}
