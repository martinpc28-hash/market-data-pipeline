"""
Fase 2 — Doble feed y normalización.

Corre Polymarket y Kalshi al mismo tiempo, cada uno en su propia tarea
async (paso 4), y además de guardar el crudo de cada uno (igual que en
Fase 1) escribe un tercer archivo con los eventos ya normalizados al
esquema común (paso 5), para que a partir de acá el resto del pipeline
no tenga que preocuparse por las diferencias entre exchanges.

Uso:
    python run_dual_feed.py \\
        --polymarket-tokens <token_id_1> <token_id_2> \\
        --kalshi-tickers <TICKER_1> <TICKER_2>

Si una de las dos conexiones falla (por ejemplo, credenciales de Kalshi
mal configuradas), la otra sigue corriendo igual — un feed caído no debe
tirar abajo la ingesta completa. Esto es una versión mínima de tolerancia
a fallos; el manejo real de reconexión y gaps es la Fase 3.
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.logging_utils import setup_logging
from common.schema import normalize
from kalshi_ingest import build_auth_headers, load_credentials, WS_PATHS  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_PING_INTERVAL_SECONDS = 10

# "¿Alberto Núñez Feijóo será el próximo Primer Ministro de España?" --
# activo, no resuelto, buena liquidez (Yes ~80.5%, ~$103k de volumen al
# 2026-08-19). A diferencia del intento anterior con mercados de España
# (que quedó documentado como NO equivalente), este SÍ es el mismo evento
# que el ticker de Kalshi de abajo -- ambos hacen literalmente la misma
# pregunta, y está confirmado y cargado como par en
# config/market_mapping.json, así que sirve directo para /spread.
DEFAULT_POLYMARKET_TOKENS = [
    "30446115040994756735976163960167805883187697808359254957130669387440187908610",   # Yes (Feijóo es el próximo PM)
    "102959860025519533768586392648393292830655850541879057833605543601433003729221",  # No
]
# KXSPANISHPM-27AUG22-AFEI: "¿Feijóo será el próximo Primer Ministro de
# España?" en Kalshi -- mismo evento que el mercado de Polymarket de
# arriba (Yes bid/ask $0.77/$0.83 al 2026-08-19, ver config/market_mapping.json).
DEFAULT_KALSHI_TICKERS = ["KXSPANISHPM-27AUG22-AFEI"]

# Fase 7: el dashboard permite buscar y "agregar" mercados nuevos vía
# POST /discover/track, que los guarda en config/tracked_markets.json en
# vez de tocar los defaults de arriba. Los mezclamos acá -- así un mercado
# agregado desde el dashboard queda trackeado la próxima vez que arranca
# la ingesta, sin tener que editar código.
def _merge_tracked_config(defaults: list[str], key: str) -> list[str]:
    path = PROJECT_ROOT / "config" / "tracked_markets.json"
    if not path.exists():
        return defaults
    try:
        extra = json.loads(path.read_text(encoding="utf-8")).get(key, [])
    except (json.JSONDecodeError, OSError):
        return defaults
    merged = list(defaults)
    for item in extra:
        if item not in merged:
            merged.append(item)
    return merged


DEFAULT_POLYMARKET_TOKENS = _merge_tracked_config(DEFAULT_POLYMARKET_TOKENS, "polymarket_tokens")
DEFAULT_KALSHI_TICKERS = _merge_tracked_config(DEFAULT_KALSHI_TICKERS, "kalshi_tickers")


def _today_path(prefix: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return DATA_DIR / f"{prefix}_{today}.jsonl"


def _write_jsonl(path: Path, record: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()


def handle_message(source: str, raw_message: str, logger) -> list[dict]:
    """Común a ambos feeds: guarda el crudo, normaliza, guarda lo normalizado.
    Cero pérdida de datos: si la normalización falla, el mensaje crudo ya
    quedó guardado antes de intentar normalizar. Devuelve los eventos
    normalizados (lista vacía si no hubo nada o falló la normalización) para
    que quien llame pueda alimentar la detección de gaps (Fase 3) sin tener
    que volver a parsear el mensaje."""
    received_at = datetime.now(timezone.utc).isoformat()

    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("[%s] Mensaje no-JSON, se guarda crudo igual: %r", source, raw_message[:200])
        payload = {"_raw_unparseable": raw_message}

    raw_record = {"_received_at": received_at, "_source": source, "payload": payload}
    _write_jsonl(_today_path(f"{source}_raw"), raw_record)

    try:
        normalized_events = normalize(source, payload, received_at)
    except Exception:
        logger.exception("[%s] Falló la normalización de un mensaje; el crudo ya está a salvo.", source)
        return []

    for event in normalized_events:
        _write_jsonl(_today_path("normalized"), event)

    return normalized_events


async def run_polymarket(tokens: list[str], logger):
    subscribe_msg = json.dumps({"assets_ids": tokens, "type": "market"})

    async def heartbeat(ws):
        try:
            while True:
                await asyncio.sleep(POLYMARKET_PING_INTERVAL_SECONDS)
                await ws.send("PING")
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass

    logger.info("[polymarket] Conectando a %s", POLYMARKET_WS_URL)
    async with websockets.connect(POLYMARKET_WS_URL, ping_interval=None) as ws:
        logger.info("[polymarket] Conectado. Suscribiendo a %d token(s).", len(tokens))
        await ws.send(subscribe_msg)

        hb_task = asyncio.create_task(heartbeat(ws))
        try:
            async for raw_message in ws:
                if raw_message == "PONG":
                    continue
                handle_message("polymarket", raw_message, logger)
        finally:
            hb_task.cancel()


async def run_kalshi(tickers: list[str], logger):
    api_key_id, private_key, env_name = load_credentials(logger)
    host, ws_path = WS_PATHS[env_name]
    ws_url = f"wss://{host}{ws_path}"
    headers = build_auth_headers(api_key_id, private_key, ws_path)

    logger.info("[kalshi] Conectando a %s (entorno: %s)", ws_url, env_name)
    async with websockets.connect(ws_url, additional_headers=headers) as ws:
        logger.info("[kalshi] Conectado. Suscribiendo a orderbook_delta para: %s", tickers)
        subscribe_msg = json.dumps({
            "id": 1,
            "cmd": "subscribe",
            "params": {"channels": ["orderbook_delta"], "market_tickers": tickers},
        })
        await ws.send(subscribe_msg)

        async for raw_message in ws:
            handle_message("kalshi", raw_message, logger)


async def run_feed_with_isolation(coro, feed_name: str, logger):
    """Envuelve cada feed para que una excepción en uno no tumbe al otro
    (asyncio.gather con return_exceptions=True hace lo mismo a nivel de
    gather, pero esto además deja un log claro de cuál feed cayó)."""
    try:
        await coro
    except Exception:
        logger.exception("[%s] El feed se cayó y no se va a reintentar en esta versión (eso es Fase 3).", feed_name)


async def main_async(polymarket_tokens, kalshi_tickers, logger):
    await asyncio.gather(
        run_feed_with_isolation(run_polymarket(polymarket_tokens, logger), "polymarket", logger),
        run_feed_with_isolation(run_kalshi(kalshi_tickers, logger), "kalshi", logger),
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Fase 2: ingesta simultánea Polymarket + Kalshi, normalizada")
    parser.add_argument("--polymarket-tokens", nargs="+", default=DEFAULT_POLYMARKET_TOKENS)
    parser.add_argument("--kalshi-tickers", nargs="+", default=DEFAULT_KALSHI_TICKERS)
    return parser.parse_args()


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    logger = setup_logging("dual_feed")
    args = parse_args()

    try:
        asyncio.run(main_async(args.polymarket_tokens, args.kalshi_tickers, logger))
    except KeyboardInterrupt:
        logger.info("Interrumpido por el usuario, cerrando ambos feeds.")


if __name__ == "__main__":
    main()
