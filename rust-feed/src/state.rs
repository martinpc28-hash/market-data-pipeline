//! Estado compartido en memoria -- lo escriben los loops de polling
//! (kalshi.rs / polymarket.rs) y lo leen las rutas HTTP (http.rs).
//!
//! Deliberadamente simple: un HashMap por plataforma detrás de un
//! Arc<RwLock<..>>, sin base de datos ni persistencia -- si el servicio
//! se reinicia, se repuebla solo en el próximo ciclo de polling (unos
//! segundos), no hace falta guardar nada en disco.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
pub struct KalshiMarket {
    pub ticker: String,
    pub event_ticker: Option<String>,
    pub title: String,
    pub yes_bid: Option<f64>,
    pub yes_ask: Option<f64>,
    pub volume_24h: Option<f64>,
    /// Segundos unix de la última actualización -- para que el lado
    /// Python pueda descartar datos viejos si el polling se atrasó
    /// (ej. el proceso se colgó pero el HTTP server sigue respondiendo).
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PolyMarket {
    pub condition_id: String,
    pub title: String,
    pub outcomes: Vec<String>,
    pub token_ids: Vec<String>,
    pub outcome_prices: Vec<String>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub volume_24h: Option<f64>,
    pub updated_at: u64,
}

#[derive(Default)]
pub struct Store {
    kalshi: RwLock<HashMap<String, KalshiMarket>>,
    polymarket: RwLock<HashMap<String, PolyMarket>>,
    /// Se marca en true la primera vez que un ciclo de polling completa
    /// con éxito para cada plataforma -- así /health puede distinguir
    /// "recién arrancando" de "algo está roto" en vez de reportar ok
    /// falso con los mapas vacíos.
    pub kalshi_ready: std::sync::atomic::AtomicBool,
    pub polymarket_ready: std::sync::atomic::AtomicBool,
}

pub type SharedStore = Arc<Store>;

pub fn new_store() -> SharedStore {
    Arc::new(Store::default())
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Store {
    pub async fn replace_kalshi(&self, markets: Vec<KalshiMarket>) {
        let mut map = HashMap::with_capacity(markets.len());
        for m in markets {
            map.insert(m.ticker.clone(), m);
        }
        *self.kalshi.write().await = map;
        self.kalshi_ready
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn replace_polymarket(&self, markets: Vec<PolyMarket>) {
        let mut map = HashMap::with_capacity(markets.len());
        for m in markets {
            map.insert(m.condition_id.clone(), m);
        }
        *self.polymarket.write().await = map;
        self.polymarket_ready
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn kalshi_snapshot(&self) -> Vec<KalshiMarket> {
        self.kalshi.read().await.values().cloned().collect()
    }

    pub async fn polymarket_snapshot(&self) -> Vec<PolyMarket> {
        self.polymarket.read().await.values().cloned().collect()
    }

    /// Todos los tickers de Kalshi conocidos actualmente -- los
    /// descubre el polling REST (kalshi.rs, que sigue corriendo, más
    /// espaciado, solo para detectar mercados nuevos); el WebSocket
    /// (kalshi_ws.rs) los usa para saber a qué suscribirse.
    pub async fn kalshi_tickers(&self) -> Vec<String> {
        self.kalshi.read().await.keys().cloned().collect()
    }

    /// Actualiza SOLO el precio de un mercado de Kalshi ya conocido (lo
    /// que manda el canal "ticker" del WebSocket) -- no toca
    /// título/volumen (eso lo completa el polling REST), y si el
    /// ticker todavía no está en el mapa (ej. llegó un mensaje antes de
    /// que el polling REST lo descubriera) simplemente no hace nada, no
    /// crea una entrada a medias sin título."
    pub async fn update_kalshi_price(&self, ticker: &str, yes_bid: Option<f64>, yes_ask: Option<f64>) {
        let mut map = self.kalshi.write().await;
        if let Some(m) = map.get_mut(ticker) {
            if yes_bid.is_some() {
                m.yes_bid = yes_bid;
            }
            if yes_ask.is_some() {
                m.yes_ask = yes_ask;
            }
            m.updated_at = now_unix();
        }
    }

    /// Todos los token_ids de Polymarket conocidos actualmente, junto
    /// con a qué condition_id pertenecen y si son el token "Yes" (índice
    /// 0 en outcomes, misma convención que _implied_yes_price_polymarket
    /// en Python) -- el WebSocket de Polymarket se suscribe a estos IDs.
    pub async fn polymarket_token_index(&self) -> Vec<(String, String, bool)> {
        let map = self.polymarket.read().await;
        let mut out = Vec::new();
        for m in map.values() {
            for (i, token_id) in m.token_ids.iter().enumerate() {
                let is_yes = m
                    .outcomes
                    .get(i)
                    .map(|o| o.trim().eq_ignore_ascii_case("yes"))
                    .unwrap_or(false);
                out.push((token_id.clone(), m.condition_id.clone(), is_yes));
            }
        }
        out
    }

    /// Actualiza SOLO el bid/ask del lado "Yes" de un mercado de
    /// Polymarket ya conocido -- igual que update_kalshi_price, no toca
    /// título/volumen, y no hace nada si el condition_id no está
    /// todavía en el mapa.
    pub async fn update_polymarket_yes_price(
        &self,
        condition_id: &str,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
    ) {
        let mut map = self.polymarket.write().await;
        if let Some(m) = map.get_mut(condition_id) {
            if best_bid.is_some() {
                m.best_bid = best_bid;
            }
            if best_ask.is_some() {
                m.best_ask = best_ask;
            }
            m.updated_at = now_unix();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_kalshi(ticker: &str) -> KalshiMarket {
        KalshiMarket {
            ticker: ticker.to_string(),
            event_ticker: Some("EVT".to_string()),
            title: "Will Bitcoin hit $150k?".to_string(),
            yes_bid: Some(4.0),
            yes_ask: Some(6.0),
            volume_24h: Some(1000.0),
            updated_at: 0,
        }
    }

    #[tokio::test]
    async fn update_kalshi_price_only_touches_price_fields() {
        let store = new_store();
        store.replace_kalshi(vec![sample_kalshi("TICK1")]).await;

        store
            .update_kalshi_price("TICK1", Some(10.0), Some(12.0))
            .await;

        let snapshot = store.kalshi_snapshot().await;
        let m = &snapshot[0];
        assert_eq!(m.yes_bid, Some(10.0));
        assert_eq!(m.yes_ask, Some(12.0));
        // el título no se toca -- lo sigue completando el polling REST
        assert_eq!(m.title, "Will Bitcoin hit $150k?");
    }

    #[tokio::test]
    async fn update_kalshi_price_on_unknown_ticker_is_a_noop() {
        let store = new_store();
        // no crea nada -- solo actualiza mercados que el polling REST ya
        // descubrió (con título/volumen), nunca una entrada a medias.
        store
            .update_kalshi_price("DESCONOCIDO", Some(10.0), Some(12.0))
            .await;
        assert!(store.kalshi_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn polymarket_token_index_flags_yes_side_correctly() {
        let store = new_store();
        store
            .replace_polymarket(vec![PolyMarket {
                condition_id: "0xabc".to_string(),
                title: "Will Bitcoin reach $150,000?".to_string(),
                outcomes: vec!["Yes".to_string(), "No".to_string()],
                token_ids: vec!["TOK_YES".to_string(), "TOK_NO".to_string()],
                outcome_prices: vec!["0.2".to_string(), "0.8".to_string()],
                best_bid: None,
                best_ask: None,
                volume_24h: Some(1000.0),
                updated_at: 0,
            }])
            .await;

        let index = store.polymarket_token_index().await;
        let yes_entry = index.iter().find(|(tok, _, _)| tok == "TOK_YES").unwrap();
        let no_entry = index.iter().find(|(tok, _, _)| tok == "TOK_NO").unwrap();
        assert!(yes_entry.2, "TOK_YES debería marcarse como lado Yes");
        assert!(!no_entry.2, "TOK_NO NO debería marcarse como lado Yes");
    }

    #[tokio::test]
    async fn update_polymarket_yes_price_only_touches_price_fields() {
        let store = new_store();
        store
            .replace_polymarket(vec![PolyMarket {
                condition_id: "0xabc".to_string(),
                title: "Will Bitcoin reach $150,000?".to_string(),
                outcomes: vec!["Yes".to_string(), "No".to_string()],
                token_ids: vec!["TOK_YES".to_string(), "TOK_NO".to_string()],
                outcome_prices: vec!["0.2".to_string(), "0.8".to_string()],
                best_bid: Some(0.19),
                best_ask: Some(0.21),
                volume_24h: Some(1000.0),
                updated_at: 0,
            }])
            .await;

        store
            .update_polymarket_yes_price("0xabc", Some(0.25), Some(0.27))
            .await;

        let snapshot = store.polymarket_snapshot().await;
        let m = &snapshot[0];
        assert_eq!(m.best_bid, Some(0.25));
        assert_eq!(m.best_ask, Some(0.27));
        assert_eq!(m.title, "Will Bitcoin reach $150,000?");
    }
}
