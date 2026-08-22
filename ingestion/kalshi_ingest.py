"""
Fase 1 (aplicada al segundo feed) — Conexión al WebSocket de Kalshi y guardado
de mensajes crudos del order book en un archivo local JSON Lines.

A diferencia de Polymarket, Kalshi exige autenticación con firma RSA-PSS en
cada conexión (ver polymarket-kalshi-websocket-research.md). Las credenciales
se leen de variables de entorno (.env) y de un archivo .pem local — nunca se
escriben en este script.

Uso:
    python kalshi_ingest.py --tickers <MARKET_TICKER_1> <MARKET_TICKER_2> ...

Requiere en la raíz del proyecto un archivo ".env" con:
    KALSHI_API_KEY_ID=tu_key_id
    KALSHI_PRIVATE_KEY_PATH=secrets/kalshi_private_key.pem
    KALSHI_ENV=demo   (o "prod")
"""

import argparse
import asyncio
import base64
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"

WS_PATHS = {
    "demo": ("external-api-ws.demo.kalshi.co", "/trade-api/ws/v2"),
    "prod": ("external-api-ws.kalshi.com", "/trade-api/ws/v2"),
}

# Ejemplo de ticker: mercado real y activo al momento de escribir esto
# ("¿la máxima en Nueva York va a superar los 92°F el 18/8/2026?"). Sirve
# para poder probar el script sin tener que buscar un ticker antes. Los
# mercados de clima expiran rápido, así que para uso real conviene pasar
# tickers propios con --tickers, buscándolos en:
# GET https://api.elections.kalshi.com/trade-api/v2/markets?status=open
DEFAULT_TICKERS = ["KXHIGHNY-26AUG18-T92"]


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("kalshi_ingest")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_DIR / "kalshi_ingest.log")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


def output_path_for_today() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return DATA_DIR / f"kalshi_raw_{today}.jsonl"


def load_credentials(logger: logging.Logger):
    """Carga KALSHI_API_KEY_ID y la clave privada desde .env + archivo .pem.
    Falla rápido y con un mensaje claro si algo falta, en vez de un traceback
    críptico más adelante."""
    load_dotenv(PROJECT_ROOT / ".env")

    api_key_id = os.environ.get("KALSHI_API_KEY_ID")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    env_name = os.environ.get("KALSHI_ENV", "demo").strip().lower()

    if not api_key_id or api_key_id == "tu_key_id_aqui":
        logger.error("Falta KALSHI_API_KEY_ID en el archivo .env (o quedó con el valor de ejemplo).")
        sys.exit(1)

    if not key_path:
        logger.error("Falta KALSHI_PRIVATE_KEY_PATH en el archivo .env.")
        sys.exit(1)

    full_key_path = PROJECT_ROOT / key_path
    if not full_key_path.exists():
        logger.error("No se encontró la clave privada en: %s", full_key_path)
        sys.exit(1)

    with full_key_path.open("rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    if env_name not in WS_PATHS:
        logger.error("KALSHI_ENV inválido: %s (usar 'demo' o 'prod')", env_name)
        sys.exit(1)

    return api_key_id, private_key, env_name


def build_auth_headers(api_key_id: str, private_key, ws_path: str) -> dict:
    """Firma timestamp + 'GET' + path con RSA-PSS/SHA-256, tal como exige la
    documentación de Kalshi. Se debe llamar de nuevo en cada reconexión,
    porque el timestamp tiene una ventana de validez corta."""
    timestamp_ms = str(int(time.time() * 1000))
    message = f"{timestamp_ms}GET{ws_path}".encode("utf-8")

    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256().digest_size),
        hashes.SHA256(),
    )
    signature_b64 = base64.b64encode(signature).decode("utf-8")

    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": signature_b64,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }


async def consume(ws: websockets.WebSocketClientProtocol, logger: logging.Logger):
    out_path = output_path_for_today()
    msg_count = 0

    with out_path.open("a", encoding="utf-8") as f:
        async for raw_message in ws:
            received_at = datetime.now(timezone.utc).isoformat()

            try:
                payload = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning("Mensaje no-JSON recibido, se guarda crudo igual: %r", raw_message[:200])
                payload = {"_raw_unparseable": raw_message}

            record = {
                "_received_at": received_at,
                "_source": "kalshi",
                "payload": payload,
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()

            # errores y confirmaciones son útiles para ver enseguida, no solo cada 50
            msg_type = payload.get("type") if isinstance(payload, dict) else None
            if msg_type in ("error", "subscribed", "unsubscribed"):
                logger.info("Mensaje de control: %s", payload)

            msg_count += 1
            if msg_count % 50 == 0:
                logger.info("Mensajes recibidos: %d (archivo: %s)", msg_count, out_path.name)


async def run(tickers: list[str], logger: logging.Logger):
    api_key_id, private_key, env_name = load_credentials(logger)
    host, ws_path = WS_PATHS[env_name]
    ws_url = f"wss://{host}{ws_path}"

    headers = build_auth_headers(api_key_id, private_key, ws_path)

    logger.info("Conectando a %s (entorno: %s)", ws_url, env_name)
    async with websockets.connect(ws_url, additional_headers=headers) as ws:
        logger.info("Conectado. Suscribiendo a orderbook_delta para: %s", tickers)
        subscribe_msg = json.dumps({
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": tickers,
            },
        })
        await ws.send(subscribe_msg)
        await consume(ws, logger)


def parse_args():
    parser = argparse.ArgumentParser(description="Ingesta Fase 1: Kalshi order book -> JSON Lines local")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=DEFAULT_TICKERS,
        help="Lista de market_tickers de Kalshi a los que suscribirse (ver GET /trade-api/v2/markets)",
    )
    return parser.parse_args()


def main():
    logger = setup_logging()
    args = parse_args()

    try:
        asyncio.run(run(args.tickers, logger))
    except KeyboardInterrupt:
        logger.info("Interrumpido por el usuario, cerrando.")
    except websockets.InvalidStatus as e:
        logger.error("El servidor rechazó la conexión (probablemente auth inválida o ticker inexistente): %s", e)
    except FileNotFoundError as e:
        logger.error("Archivo no encontrado: %s", e)


if __name__ == "__main__":
    main()