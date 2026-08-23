//! API HTTP local que consume el backend Python -- ver
//! tools/market_matcher.py, que intenta esto primero y cae a las APIs
//! externas de Kalshi/Polymarket si el servicio no responde (mismo
//! criterio de fallback usado en todo el resto del proyecto).

use crate::state::SharedStore;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use std::sync::atomic::Ordering;

pub fn router(store: SharedStore) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/prices/kalshi", get(kalshi_prices))
        .route("/prices/polymarket", get(polymarket_prices))
        .with_state(store)
}

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    kalshi_ready: bool,
    polymarket_ready: bool,
}

async fn health(State(store): State<SharedStore>) -> Json<HealthBody> {
    Json(HealthBody {
        status: "ok",
        kalshi_ready: store.kalshi_ready.load(Ordering::Relaxed),
        polymarket_ready: store.polymarket_ready.load(Ordering::Relaxed),
    })
}

async fn kalshi_prices(State(store): State<SharedStore>) -> Json<Vec<crate::state::KalshiMarket>> {
    Json(store.kalshi_snapshot().await)
}

async fn polymarket_prices(
    State(store): State<SharedStore>,
) -> Json<Vec<crate::state::PolyMarket>> {
    Json(store.polymarket_snapshot().await)
}
