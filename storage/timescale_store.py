"""
Fase 4 — Persistencia de los eventos normalizados en TimescaleDB.

Cada evento normalizado (ver common/schema.py) se inserta tal cual en la
hypertable `market_events`, sin transformarlo -- la tabla tiene una columna
por cada campo que puede aparecer en cualquiera de los dos exchanges, y las
que no aplican para un evento dado quedan NULL. Esto evita tener que decidir
de antemano un esquema "mínimo común" que perdería información específica
de cada exchange (la misma filosofía de common/schema.py: preservar en vez
de forzar a que se vean iguales). La columna `raw` guarda el evento
normalizado completo por si algún día hace falta un campo que no tiene
columna propia.

price / size / size_delta se guardan como NUMERIC (no FLOAT), convertidos
desde el string decimal que ya viene en el evento normalizado, para no
introducir error de punto flotante en datos financieros -- la misma
convención de "decimal como string" que se usa en todo el pipeline.

La escritura es asíncrona (asyncpg) y por lotes: ver
ingestion/run_resilient_feed.py, que junta eventos en una cola en memoria y
los manda acá de a bloques en vez de un INSERT por mensaje. A la frecuencia
de mensajes de un order book activo, un round-trip a la base por cada
mensaje sería un cuello de botella innecesario y además reintroduciría el
riesgo de que un problema de storage frene la ingesta en tiempo real.

Fase 5 agrega las queries de LECTURA que usa la API (api/main.py):
list_outcomes (qué hay para consultar), fetch_book_events (los eventos
necesarios para reconstruir el book vivo de un outcome con
common/orderbook.py) y fetch_history (histórico paginado, crudo).
"""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg

INSERT_SQL = """
INSERT INTO market_events (
    received_at, exchange_ts, source, market_id, outcome_id, event_type,
    update_kind, book_side, side, price, size, size_delta, sequence,
    bids, asks, raw
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
    $14::jsonb, $15::jsonb, $16::jsonb
)
"""


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _to_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _to_jsonb(value: Any) -> str:
    if value is None:
        return "null"
    return json.dumps(value, ensure_ascii=False)


def _parse_delete_count(result: str) -> int:
    """asyncpg devuelve el resultado de un DELETE como el status tag de
    Postgres, ej. "DELETE 42" -- se parsea el número de filas borradas."""
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return 0


def _row_to_event(r) -> dict:
    """Convierte un renglón de la tabla de vuelta a la forma de evento
    normalizado (ver common/schema.py). El precio se vuelve string (no
    Decimal) a propósito: common/orderbook.py usa el precio como clave de
    diccionario, y las claves que vienen en los arrays bids/asks de un
    book_snapshot ya son strings (así las guardó common/schema.py) -- si
    acá dejáramos Decimal, "0.73" (del snapshot) y Decimal("0.73") (de un
    update leído de la base) serían claves distintas para Python aunque
    representen el mismo precio, y la reconstrucción del book quedaría
    rota en silencio."""
    return {
        "received_at": r["received_at"].isoformat() if r["received_at"] else None,
        "exchange_ts": r["exchange_ts"].isoformat() if r["exchange_ts"] else None,
        "source": r["source"],
        "market_id": r["market_id"],
        "outcome_id": r["outcome_id"],
        "event_type": r["event_type"],
        "update_kind": r["update_kind"],
        "book_side": r["book_side"],
        "side": r["side"],
        "price": str(r["price"]) if r["price"] is not None else None,
        "size": str(r["size"]) if r["size"] is not None else None,
        "size_delta": str(r["size_delta"]) if r["size_delta"] is not None else None,
        "sequence": r["sequence"],
        "bids": json.loads(r["bids"]) if r["bids"] else None,
        "asks": json.loads(r["asks"]) if r["asks"] else None,
    }


def dsn_from_env() -> str:
    host = os.environ.get("TIMESCALE_HOST", "localhost")
    port = os.environ.get("TIMESCALE_PORT", "5432")
    db = os.environ.get("TIMESCALE_DB", "market_data")
    user = os.environ.get("TIMESCALE_USER", "market_data")
    password = os.environ.get("TIMESCALE_PASSWORD", "market_data")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


class TimescaleStore:
    """Pool de conexiones + inserts por lote. Un solo pool para todo el
    proceso, creado una vez al arrancar el pipeline (ver connect())."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)

    async def close(self):
        if self._pool is not None:
            await self._pool.close()

    async def insert_events(self, events: list[dict]) -> int:
        """Inserta un lote de eventos ya normalizados. Devuelve cuántos se
        insertaron. Con lote vacío no toca la base (evita un round-trip
        inútil cuando el watchdog de flush dispara sin nada acumulado)."""
        if not events:
            return 0
        if self._pool is None:
            raise RuntimeError("TimescaleStore.connect() no fue llamado todavía")

        rows = [
            (
                _to_ts(e.get("received_at")),
                _to_ts(e.get("exchange_ts")),
                e["source"],
                e.get("market_id"),
                e.get("outcome_id"),
                e["event_type"],
                e.get("update_kind"),
                e.get("book_side"),
                e.get("side"),
                _to_decimal(e.get("price")),
                _to_decimal(e.get("size")),
                _to_decimal(e.get("size_delta")),
                e.get("sequence"),
                _to_jsonb(e.get("bids")),
                _to_jsonb(e.get("asks")),
                _to_jsonb(e.get("raw")),
            )
            for e in events
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(INSERT_SQL, rows)
        return len(rows)

    async def list_outcomes(self) -> list[dict]:
        """Un renglón por outcome_id visto alguna vez, con el último evento
        que le llegó -- para que la API tenga de dónde armar /markets sin
        que el usuario tenga que saber de antemano qué outcome_id pedir."""
        sql = """
            SELECT DISTINCT ON (outcome_id)
                source, market_id, outcome_id, event_type AS last_event_type,
                received_at AS last_seen
            FROM market_events
            WHERE outcome_id IS NOT NULL
            ORDER BY outcome_id, received_at DESC
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql)
        return [
            {
                "source": r["source"],
                "market_id": r["market_id"],
                "outcome_id": r["outcome_id"],
                "last_event_type": r["last_event_type"],
                "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
            }
            for r in rows
        ]

    async def fetch_book_events(self, outcome_id: str, before: datetime | None = None,
                                 max_events: int = 5000) -> list[dict]:
        """Devuelve los eventos necesarios para reconstruir el book de
        `outcome_id` en el instante `before` (default: ahora) --
        el book_snapshot más reciente anterior a ese instante, más todos
        los book_update posteriores a ese snapshot hasta `before`, en
        orden ascendente (listos para pasarle directo a
        common.orderbook.reconstruct_book). Si no hay ningún snapshot
        previo, arranca desde el primer evento disponible -- el resultado
        en ese caso puede estar incompleto (ver docstring de orderbook.py),
        cosa que la API deja explícita en su respuesta, no la esconde."""
        before = before or datetime.now(timezone.utc)

        async with self._pool.acquire() as conn:
            snapshot_ts = await conn.fetchval(
                """
                SELECT received_at FROM market_events
                WHERE outcome_id = $1 AND event_type = 'book_snapshot' AND received_at <= $2
                ORDER BY received_at DESC LIMIT 1
                """,
                outcome_id, before,
            )

            if snapshot_ts is not None:
                rows = await conn.fetch(
                    """
                    SELECT * FROM market_events
                    WHERE outcome_id = $1 AND received_at >= $2 AND received_at <= $3
                      AND event_type IN ('book_snapshot', 'book_update')
                    ORDER BY received_at ASC LIMIT $4
                    """,
                    outcome_id, snapshot_ts, before, max_events,
                )
                had_snapshot = True
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM market_events
                    WHERE outcome_id = $1 AND received_at <= $2
                      AND event_type IN ('book_snapshot', 'book_update')
                    ORDER BY received_at ASC LIMIT $3
                    """,
                    outcome_id, before, max_events,
                )
                had_snapshot = False

        events = [_row_to_event(r) for r in rows]
        for e in events:
            e["_had_snapshot"] = had_snapshot
        return events

    async def fetch_history(self, outcome_id: str, start: datetime | None = None,
                             end: datetime | None = None, limit: int = 500) -> list[dict]:
        """Histórico crudo (sin reconstruir el book) de un outcome_id en un
        rango de tiempo, más reciente primero. Pensado para graficar o
        auditar, no para derivar el book -- para eso está
        fetch_book_events()."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM market_events
                WHERE outcome_id = $1
                  AND ($2::timestamptz IS NULL OR received_at >= $2)
                  AND ($3::timestamptz IS NULL OR received_at <= $3)
                ORDER BY received_at DESC
                LIMIT $4
                """,
                outcome_id, start, end, limit,
            )
        return [_row_to_event(r) for r in rows]

    async def delete_by_outcome(self, outcome_id: str) -> int:
        """Borra TODO el historial de un outcome_id -- usado por
        POST /discover/untrack (Fase 7, botón "🗑" de "Mercados
        trackeados") para que un mercado desaparezca del dashboard al
        toque, sin esperar a que la ingesta se reinicie. Solo saca el
        outcome de ESTE outcome_id puntual -- para Polymarket (un
        token_id = un outcome, Yes o No por separado) es lo que
        corresponde; para Kalshi conviene delete_by_market (ver abajo),
        que borra los dos lados (yes/no) de un mismo ticker de una."""
        async with self._pool.acquire() as conn:
            result = await conn.execute("DELETE FROM market_events WHERE outcome_id = $1", outcome_id)
        return _parse_delete_count(result)

    async def delete_by_market(self, market_id: str) -> int:
        """Como delete_by_outcome, pero por market_id -- borra TODOS los
        outcome_id de ese mercado de una (para Kalshi, donde un ticker
        puede tener eventos bajo "TICKER:yes" y "TICKER:no", ver
        common/schema.py)."""
        async with self._pool.acquire() as conn:
            result = await conn.execute("DELETE FROM market_events WHERE market_id = $1", market_id)
        return _parse_delete_count(result)
