"""
Fase 2, paso 5 — Normalización a un esquema común.

Decisión de diseño importante (documentar en el README de la Fase 6):
Polymarket y Kalshi NO representan el order book de la misma forma, y
normalizar de más (forzarlos a verse idénticos) escondería esa diferencia
en vez de manejarla explícitamente. Por eso este módulo normaliza el
"sobre" del mensaje (timestamps, nombres de campo, identificadores) pero
preserva la semántica nativa de cada update:

- Polymarket "book": snapshot de dos lados (bids Y asks) para un asset_id
  (cada outcome de un mercado binario es un asset_id separado, con su
  propio book completo).
- Polymarket "price_change": el nuevo tamaño ABSOLUTO en un nivel de
  precio (no un delta). size="0" significa que ese nivel se vació.
- Kalshi "orderbook_snapshot": hasta dos arrays (yes_dollars_fp /
  no_dollars_fp), cada uno representando solo el lado de compra (bids)
  de ESE outcome — Kalshi no manda un array de asks separado, el ask de
  un outcome es matemáticamente el bid del outcome contrario (1 - precio).
- Kalshi "orderbook_delta": un DELTA firmado (puede ser negativo) que se
  aplica sobre el tamaño ya existente en ese nivel, más un `seq` que
  Polymarket no tiene.

Reconstruir el book "vivo" en memoria (aplicando estos updates sobre un
estado local) es trabajo de la Fase 3/4, no de este módulo. Acá solo
normalizamos la forma del evento para que el resto del pipeline pueda
iterar sobre una lista de eventos con nombres de campo consistentes,
sin tener que saber de qué exchange vino cada uno hasta que le importe.

Los precios y tamaños se mantienen como string decimal (nunca float) en
todo el pipeline, para no perder precisión con operaciones de punto
flotante en datos financieros.
"""

from datetime import datetime, timezone
from typing import Any


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _polymarket_ts_to_iso(ts_str: str) -> str:
    """Polymarket manda timestamps como string de epoch en milisegundos."""
    return _iso(datetime.fromtimestamp(int(ts_str) / 1000, tz=timezone.utc))


def _kalshi_ts_to_iso(ts_field: Any) -> str | None:
    """Kalshi manda 'ts' como ISO string (en los deltas) o no lo manda en
    absoluto (en el snapshot inicial, donde no hay campo de tiempo del
    exchange, solo lo que nosotros recibimos)."""
    if ts_field is None:
        return None
    # Ya viene en formato ISO 8601 (con 'Z'); normalizamos el sufijo.
    return ts_field.replace("Z", "+00:00")


def normalize_polymarket(payload: Any, received_at: str) -> list[dict]:
    """payload puede ser un dict (un evento) o una lista de dicts (Polymarket
    manda una lista cuando te suscribís a varios assets_ids y llega el
    snapshot inicial de todos juntos)."""
    events = payload if isinstance(payload, list) else [payload]
    normalized = []

    for event in events:
        if not isinstance(event, dict):
            continue

        event_type = event.get("event_type")

        if event_type == "book":
            normalized.append({
                "source": "polymarket",
                "received_at": received_at,
                "exchange_ts": _polymarket_ts_to_iso(event["timestamp"]),
                "market_id": event.get("market"),
                "outcome_id": event.get("asset_id"),
                "event_type": "book_snapshot",
                "book_side": "two_sided",
                "bids": event.get("bids", []),
                "asks": event.get("asks", []),
                "raw": event,
            })

        elif event_type == "price_change":
            exchange_ts = _polymarket_ts_to_iso(event["timestamp"])
            for change in event.get("price_changes", []):
                normalized.append({
                    "source": "polymarket",
                    "received_at": received_at,
                    "exchange_ts": exchange_ts,
                    "market_id": event.get("market"),
                    "outcome_id": change.get("asset_id"),
                    "event_type": "book_update",
                    "update_kind": "absolute_replace",  # ver docstring del módulo
                    "side": "bid" if change.get("side") == "BUY" else "ask",
                    "price": change.get("price"),
                    "size": change.get("size"),
                    "sequence": None,  # Polymarket no expone número de secuencia
                    "raw": event,
                })

        else:
            # tick_size_change, last_trade_price, best_bid_ask, new_market,
            # market_resolved, etc. Los pasamos igual, sin descartarlos
            # silenciosamente (cero pérdida de datos), pero sin forzarlos a
            # un esquema que no les corresponde todavía.
            normalized.append({
                "source": "polymarket",
                "received_at": received_at,
                "exchange_ts": _polymarket_ts_to_iso(event["timestamp"]) if event.get("timestamp") else None,
                "market_id": event.get("market"),
                "outcome_id": event.get("asset_id"),
                "event_type": f"other:{event_type}",
                "raw": event,
            })

    return normalized


def normalize_kalshi(payload: dict, received_at: str) -> list[dict]:
    if not isinstance(payload, dict):
        return []

    msg_type = payload.get("type")
    msg = payload.get("msg", {})
    normalized = []

    if msg_type == "orderbook_snapshot":
        ticker = msg.get("market_ticker")
        for side_key, side_name in (("yes_dollars_fp", "yes"), ("no_dollars_fp", "no")):
            levels = msg.get(side_key)
            if not levels:
                continue  # Kalshi puede omitir el array si ese lado no tiene órdenes
            bids = [{"price": str(price), "size": str(size)} for price, size in levels]
            normalized.append({
                "source": "kalshi",
                "received_at": received_at,
                "exchange_ts": None,  # el snapshot no trae timestamp propio del exchange
                "market_id": ticker,
                "outcome_id": f"{ticker}:{side_name}",
                "event_type": "book_snapshot",
                "book_side": "bid_only",  # ver docstring: Kalshi no manda asks explícitos
                "bids": bids,
                "asks": [],
                "sequence": payload.get("seq"),
                "raw": payload,
            })

    elif msg_type == "orderbook_delta":
        ticker = msg.get("market_ticker")
        side_name = msg.get("side")
        normalized.append({
            "source": "kalshi",
            "received_at": received_at,
            "exchange_ts": _kalshi_ts_to_iso(msg.get("ts")),
            "market_id": ticker,
            "outcome_id": f"{ticker}:{side_name}",
            "event_type": "book_update",
            "update_kind": "delta_increment",  # ver docstring del módulo
            "side": "bid",  # el book de Kalshi por outcome es siempre de compra
            "price": msg.get("price_dollars"),
            "size_delta": msg.get("delta_fp"),
            "sequence": payload.get("seq"),
            "raw": payload,
        })

    else:
        # subscribed, unsubscribed, error, ticker, trade, etc. — no se
        # descartan, pero tampoco se fuerzan a "book_*".
        normalized.append({
            "source": "kalshi",
            "received_at": received_at,
            "exchange_ts": None,
            "market_id": msg.get("market_ticker") if isinstance(msg, dict) else None,
            "outcome_id": None,
            "event_type": f"other:{msg_type}",
            "raw": payload,
        })

    return normalized


def normalize(source: str, payload: Any, received_at: str) -> list[dict]:
    if source == "polymarket":
        return normalize_polymarket(payload, received_at)
    if source == "kalshi":
        return normalize_kalshi(payload, received_at)
    raise ValueError(f"Fuente desconocida: {source}")
