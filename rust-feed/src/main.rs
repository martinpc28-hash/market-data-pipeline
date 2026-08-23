//! crypto_live_feed -- servicio chico en Rust que mantiene un snapshot
//! "en vivo" de los mercados de cripto en Kalshi y Polymarket, y lo
//! expone por HTTP en el puerto 8090 para que tools/market_matcher.py
//! (Python) lo consuma en vez de pegarle directo a las APIs externas en
//! cada búsqueda del dashboard.
//!
//! Nace de una charla puntual con el usuario sobre un proyecto de
//! GitHub (poly-kalshi-arb) que usa WebSockets en vivo para emparejar
//! candidatos de arbitraje al instante -- ACLARACIÓN IMPORTANTE (ver
//! charla con el usuario): de ese proyecto solo se adoptó la idea de
//! matching/precio en tiempo real, nunca la ejecución automática de
//! órdenes con dinero real -- este servicio solo lee y expone precios,
//! nunca coloca operaciones. Eso queda, como todo lo demás del
//! dashboard, en manos del usuario confirmando a mano.
//!
//! Dos fuentes combinadas por plataforma:
//!   - Polling REST (kalshi.rs / polymarket.rs) -- DESCUBRE qué
//!     mercados existen (título, ticker/condition_id, volumen). Sigue
//!     corriendo siempre, no requiere credenciales.
//!   - WebSocket en vivo (kalshi_ws.rs / polymarket_ws.rs) -- actualiza
//!     el bid/ask de los mercados que el polling ya descubrió, en
//!     cuanto la plataforma lo publica (no espera al próximo ciclo de
//!     polling). Polymarket es público, sin credenciales. Kalshi exige
//!     autenticación firmada (RSA-PSS, ver kalshi_auth.rs) con las
//!     MISMAS credenciales que ya usa ingestion/kalshi_ingest.py
//!     (KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH) -- si no están
//!     configuradas en este contenedor, kalshi_ws simplemente no
//!     arranca y el servicio sigue funcionando solo con polling REST
//!     (nunca es un requisito duro).
//!
//! Alcance deliberadamente chico por ahora: solo cripto/Bitcoin, no
//! todas las categorías (ver TAG_SLUGS en polymarket.rs y el filtro de
//! categoría "Crypto" en kalshi.rs).

mod http;
mod kalshi;
mod kalshi_auth;
mod kalshi_ws;
mod polymarket;
mod polymarket_ws;
mod state;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crypto_live_feed=info".into()),
        )
        .init();

    let store = state::new_store();

    tracing::info!("crypto_live_feed arrancando -- kalshi + polymarket, solo cripto por ahora");

    tokio::spawn(kalshi::run(store.clone()));
    tokio::spawn(polymarket::run(store.clone()));
    // Los WebSockets arrancan un toque más tarde, para dejarle al
    // polling REST una primera pasada y tener tickers/tokens que
    // suscribir (si no, la primera conexión falla con "sin tickers
    // descubiertos todavía" y reintenta a los 5s -- funciona igual,
    // esto solo evita ese primer reintento innecesario).
    let kalshi_ws_store = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        kalshi_ws::run(kalshi_ws_store).await;
    });
    let poly_ws_store = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        polymarket_ws::run(poly_ws_store).await;
    });

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8090);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "crypto_live_feed escuchando");

    axum::serve(listener, http::router(store)).await?;
    Ok(())
}
