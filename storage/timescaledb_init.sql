-- Fase 4 — Inicialización de la hypertable de TimescaleDB.
-- Este script corre automáticamente la primera vez que se levanta el
-- contenedor de timescaledb (Postgres lo ejecuta solo si el volumen de
-- datos está vacío -- ver docker-compose.yml, se monta en
-- /docker-entrypoint-initdb.d/).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Una tabla ancha con una columna por cada campo que puede aparecer en
-- cualquiera de los eventos normalizados (ver common/schema.py) en vez de
-- varias tablas por tipo de evento: los distintos event_type comparten la
-- gran mayoría de las columnas, y separarlas complicaría las consultas de
-- Fase 5 (precio actual, histórico, spread) sin ganar mucho a cambio.
-- `raw` guarda el evento normalizado completo como respaldo.
CREATE TABLE IF NOT EXISTS market_events (
    id BIGSERIAL,
    received_at TIMESTAMPTZ NOT NULL,
    exchange_ts TIMESTAMPTZ,
    source TEXT NOT NULL,
    market_id TEXT,
    outcome_id TEXT,
    event_type TEXT NOT NULL,
    update_kind TEXT,
    book_side TEXT,
    side TEXT,
    price NUMERIC,
    size NUMERIC,
    size_delta NUMERIC,
    sequence BIGINT,
    bids JSONB,
    asks JSONB,
    raw JSONB NOT NULL,
    -- La primary key de una hypertable tiene que incluir la columna de
    -- particionamiento (received_at); por eso es compuesta con id.
    PRIMARY KEY (id, received_at)
);

SELECT create_hypertable(
    'market_events', 'received_at',
    if_not_exists => TRUE,
    -- Un chunk por día: suficiente granularidad para un proyecto de este
    -- volumen sin generar de más (cada chunk es una tabla física aparte).
    chunk_time_interval => INTERVAL '1 day'
);

-- Las dos consultas que va a necesitar la API de Fase 5: "último precio de
-- este outcome" (ORDER BY received_at DESC LIMIT 1 filtrando por
-- outcome_id) e "histórico de un outcome en un rango de tiempo".
CREATE INDEX IF NOT EXISTS idx_market_events_outcome_time
    ON market_events (outcome_id, received_at DESC);

-- Para consultas por exchange completo (ej: "todo lo que llegó de Kalshi
-- en la última hora", útil para debugging y para el reporte de stress
-- testing del README de Fase 6).
CREATE INDEX IF NOT EXISTS idx_market_events_source_time
    ON market_events (source, received_at DESC);
