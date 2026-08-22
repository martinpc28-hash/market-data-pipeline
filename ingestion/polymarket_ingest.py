"""
Fase 1 — Ingesta básica: conexión a un solo feed (Polymarket) y guardado
de mensajes crudos en un archivo local JSON Lines.

Uso:
    python polymarket_ingest.py --tokens <token_id_1> <token_id_2> ...

Si no se pasan --tokens, usa un set de ejemplo (mercados con volumen reciente
en Polymarket) solo para poder correr el script de punta a punta sin tener
que ir a buscar un token_id primero.

Referencia de la API: ver polymarket-kalshi-websocket-research.md
- Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
- Sin autenticación.
- Heartbeat de aplicación: enviar el texto "PING" cada 10s, el server responde "PONG".
- Evento relevante: "book" (snapshot completo del order book de un asset/token).
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_SECONDS = 10

# Tokens de ejemplo (par YES/NO de un mercado con actividad reciente al momento
# de escribir esto). Sirven solo para probar el script sin depender de que el
# usuario ya tenga un token_id a mano. En Fase 2+ estos vendrán de config/CLI.
DEFAULT_TOKENS = [
    "91139635598592075206531191346543584071609481550130776528207384380881068962215",
    "64453625603605254052217604785267319625453134777497489021641476848308831825427",
]

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("polymarket_ingest")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_DIR / "polymarket_ingest.log")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


def output_path_for_today() -> Path:
    """Un archivo JSONL por día, para no tener un solo archivo creciendo sin
    límite. En fases posteriores esto se reemplaza por particionado real en
    MinIO (por fecha y mercado), pero para Fase 1 alcanza con esto."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return DATA_DIR / f"polymarket_raw_{today}.jsonl"


async def send_heartbeat(ws: websockets.WebSocketClientProtocol, logger: logging.Logger):
    """Envía PING cada 10s mientras la conexión esté viva. Es un heartbeat de
    aplicación, no el ping/pong nativo del protocolo WS — Polymarket lo exige
    así explícitamente."""
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            await ws.send("PING")
            logger.debug("PING enviado")
    except asyncio.CancelledError:
        pass
    except websockets.ConnectionClosed:
        pass


async def consume(ws: websockets.WebSocketClientProtocol, logger: logging.Logger):
    """Lee mensajes del socket y los escribe crudos (sin transformar) a un
    archivo JSON Lines, uno por línea, con un timestamp de recepción propio
    además del que trae el mensaje (para poder auditar latencia/gaps después)."""
    out_path = output_path_for_today()
    msg_count = 0

    with out_path.open("a", encoding="utf-8") as f:
        async for raw_message in ws:
            # El servidor puede responder "PONG" como texto plano al heartbeat;
            # eso no es un mensaje de datos, así que lo filtramos.
            if raw_message == "PONG":
                logger.debug("PONG recibido")
                continue

            received_at = datetime.now(timezone.utc).isoformat()

            try:
                payload = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning("Mensaje no-JSON recibido, se guarda crudo igual: %r", raw_message[:200])
                payload = {"_raw_unparseable": raw_message}

            record = {
                "_received_at": received_at,
                "_source": "polymarket",
                "payload": payload,
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()  # Fase 1: preferimos durabilidad a throughput.

            msg_count += 1
            if msg_count % 50 == 0:
                logger.info("Mensajes recibidos: %d (archivo: %s)", msg_count, out_path.name)


async def run(tokens: list[str], logger: logging.Logger):
    subscribe_msg = json.dumps({"assets_ids": tokens, "type": "market"})

    logger.info("Conectando a %s", WS_URL)
    async with websockets.connect(WS_URL, ping_interval=None) as ws:
        logger.info("Conectado. Suscribiendo a %d token(s): %s", len(tokens), tokens)
        await ws.send(subscribe_msg)

        heartbeat_task = asyncio.create_task(send_heartbeat(ws, logger))
        try:
            await consume(ws, logger)
        finally:
            heartbeat_task.cancel()


def parse_args():
    parser = argparse.ArgumentParser(description="Ingesta Fase 1: Polymarket order book -> JSON Lines local")
    parser.add_argument(
        "--tokens",
        nargs="+",
        default=DEFAULT_TOKENS,
        help="Lista de token_ids (asset_ids) de Polymarket CLOB a los que suscribirse",
    )
    return parser.parse_args()


def main():
    logger = setup_logging()
    args = parse_args()

    try:
        asyncio.run(run(args.tokens, logger))
    except KeyboardInterrupt:
        logger.info("Interrumpido por el usuario, cerrando.")


if __name__ == "__main__":
    main()