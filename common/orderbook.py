"""
Fase 5, paso base — Reconstrucción del order book "vivo" a partir de la
secuencia de eventos guardados (un book_snapshot seguido de sus
book_update posteriores). Este módulo es puro: no toca la base de datos,
solo recibe una lista de eventos ya traídos y devuelve el estado
resultante. La consulta a TimescaleDB que junta esos eventos vive en
storage/timescale_store.py (fetch_book_events).

Por qué hace falta esto (y no alcanza con el último book_update): ni
Polymarket ni Kalshi mandan "el precio actual" como un campo suelto en
cada mensaje -- lo que mandan es una foto completa (snapshot) y después
parches sobre esa foto (updates). El precio actual/mejor oferta hay que
derivarlo aplicando esos parches en orden, exactamente como lo haría un
cliente que mantiene el book en memoria en tiempo real. Ver el docstring
de common/schema.py para la semántica de cada tipo de update -- este
módulo es la implementación de lo que ahí quedó pendiente para más
adelante ("Reconstruir el book vivo... es trabajo de la Fase 3/4").

Importante -- asimetría entre exchanges, documentada para no esconderla:
- Polymarket: book_snapshot trae bids Y asks. Cada book_update es un
  reemplazo ABSOLUTO del tamaño en ese nivel de precio (size="0" vacía el
  nivel).
- Kalshi: cada outcome_id (ej: "TICKER:yes") solo tiene bids -- el ask de
  ese outcome es matemáticamente 1 - bid del outcome contrario, Kalshi no
  lo manda explícito. Cada book_update es un DELTA que se SUMA al tamaño
  ya existente en ese nivel (puede ser negativo).
Por eso best_ask() puede devolver None para un outcome_id de Kalshi -- no
es un bug, es que ese dato no existe en ese lado del par bid_only. Quien
consuma este módulo (la API) tiene que calcular el ask de Kalshi a partir
del bid del outcome contrario si lo necesita, no inventarlo acá.
"""

from decimal import Decimal, InvalidOperation
from typing import Any


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def reconstruct_book(events: list[dict]) -> dict:
    """events: lista de eventos normalizados (los mismos dicts que guarda
    TimescaleDB), ordenados por received_at ASCENDENTE, empezando por un
    book_snapshot (si el primero no es un snapshot, el resultado arranca
    de un book vacío -- puede pasar si se pide un rango que no incluye
    ningún snapshot, y es preferible devolver "no tengo suficiente
    información" implícito antes que inventar un estado).

    Devuelve: {"bids": {price_str: Decimal}, "asks": {price_str: Decimal},
    "as_of": <received_at del último evento aplicado>, "n_events": int}
    """
    bids: dict[str, Decimal] = {}
    asks: dict[str, Decimal] = {}
    as_of = None
    n_events = 0

    for event in events:
        event_type = event.get("event_type")

        if event_type == "book_snapshot":
            bids = {
                lvl["price"]: _to_decimal(lvl["size"]) or Decimal(0)
                for lvl in event.get("bids") or []
            }
            asks = {
                lvl["price"]: _to_decimal(lvl["size"]) or Decimal(0)
                for lvl in event.get("asks") or []
            }

        elif event_type == "book_update":
            price = event.get("price")
            side = event.get("side")
            update_kind = event.get("update_kind")
            target = bids if side == "bid" else asks

            if update_kind == "absolute_replace":
                size = _to_decimal(event.get("size"))
                if size is None or size == 0:
                    target.pop(price, None)
                else:
                    target[price] = size

            elif update_kind == "delta_increment":
                delta = _to_decimal(event.get("size_delta")) or Decimal(0)
                new_size = target.get(price, Decimal(0)) + delta
                if new_size <= 0:
                    target.pop(price, None)
                else:
                    target[price] = new_size
            # otros update_kind desconocidos se ignoran para la reconstrucción
            # (no deberían aparecer -- common/schema.py solo produce estos dos)

        # los "other:*" (last_trade_price, subscribed, etc.) no tocan el
        # book, solo se saltean acá -- ya están a salvo en la tabla igual.

        n_events += 1
        as_of = event.get("received_at")

    return {"bids": bids, "asks": asks, "as_of": as_of, "n_events": n_events}


def best_bid(book: dict) -> Decimal | None:
    prices = [_to_decimal(p) for p in book["bids"] if book["bids"][p] > 0]
    prices = [p for p in prices if p is not None]
    return max(prices) if prices else None


def best_ask(book: dict) -> Decimal | None:
    prices = [_to_decimal(p) for p in book["asks"] if book["asks"][p] > 0]
    prices = [p for p in prices if p is not None]
    return min(prices) if prices else None


def midpoint(book: dict) -> Decimal | None:
    bid, ask = best_bid(book), best_ask(book)
    if bid is not None and ask is not None:
        return (bid + ask) / 2
    return None
