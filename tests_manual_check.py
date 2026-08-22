"""Chequeo manual rápido (no es el suite de tests de la Fase 7 todavía):
corre el normalizador sobre mensajes reales que ya capturamos en las
pruebas en vivo, para confirmar que no hay errores de parseo antes de
integrarlo al pipeline de doble feed."""

import json
from common.schema import normalize

polymarket_book = {
    "event_type": "book", "market": "0x819a...", "asset_id": "9113...",
    "timestamp": "1787061330692", "hash": "abc",
    "bids": [], "asks": [{"price": "0.999", "size": "99999"}],
}

polymarket_price_change = {
    "event_type": "price_change", "market": "0x819a...",
    "price_changes": [
        {"asset_id": "6445...", "price": "0.013", "size": "44000", "side": "BUY"},
        {"asset_id": "9113...", "price": "0.987", "size": "44000", "side": "SELL"},
    ],
    "timestamp": "1787061351465",
}

kalshi_snapshot = {
    "type": "orderbook_snapshot", "sid": 1, "seq": 1,
    "msg": {
        "market_ticker": "KXHIGHNY-26AUG18-T92",
        "no_dollars_fp": [["0.0100", "855.00"], ["0.0200", "20.00"]],
    },
}

kalshi_delta = {
    "type": "orderbook_delta", "sid": 1, "seq": 2,
    "msg": {
        "market_ticker": "KXHIGHNY-26AUG18-T92",
        "price_dollars": "0.9500", "delta_fp": "-5.00", "side": "no",
        "ts": "2026-08-18T18:05:49.68146Z",
    },
}

for label, source, payload in [
    ("Polymarket book", "polymarket", polymarket_book),
    ("Polymarket price_change", "polymarket", polymarket_price_change),
    ("Kalshi snapshot", "kalshi", kalshi_snapshot),
    ("Kalshi delta", "kalshi", kalshi_delta),
]:
    print(f"--- {label} ---")
    for ev in normalize(source, payload, received_at="2026-08-18T18:05:41.000000+00:00"):
        ev_no_raw = {k: v for k, v in ev.items() if k != "raw"}
        print(json.dumps(ev_no_raw, ensure_ascii=False))
    print()
