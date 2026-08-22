"""
Fase 3 — Tolerancia a fallos (la parte más importante del proyecto).

Extiende run_dual_feed.py (Fase 2) con:
  - Paso 7: reconexión automática con backoff exponencial por feed.
  - Paso 8: detección de gaps (secuencia para Kalshi, silencio para Polymarket).
  - Paso 9: cada gap se loggea con inicio/fin en logs/gaps.log y logs/gaps.jsonl.
  - Paso 10: flags --chaos-kill-after-* para simular fallos de forma
    reproducible, sin depender de cortar el wifi a mano cada vez.

Uso normal:
    python run_resilient_feed.py

Simulando un corte de Polymarket a los 20s de conectado, uno solo (no se
repite), para confirmar que reconecta solo y que el gap queda documentado:
    python run_resilient_feed.py --chaos-kill-after-polymarket 20

Simulando cortes en AMBOS feeds, cada uno una vez:
    python run_resilient_feed.py --chaos-kill-after-polymarket 20 --chaos-kill-after-kalshi 25

Fase 4 agrega storage: los eventos normalizados se escriben a TimescaleDB
(en lotes, vía una cola en memoria) y los JSONL crudos se archivan
periódicamente en MinIO. Requiere `docker compose up -d` corriendo (ver
docker-compose.yml). Si no tenés Docker levantado y solo querés probar la
parte de ingesta/reconexión, usá --no-storage:
    python run_resilient_feed.py --no-storage
Un fallo conectando a TimescaleDB o MinIO al arrancar NUNCA frena la
ingesta: se loggea como advertencia y el pipeline sigue guardando el JSONL
local igual que en Fase 1-3, solo que sin la copia en base de
datos/object storage hasta que se resuelva.
"""

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.logging_utils import setup_logging, gap_logger as get_gap_logger
from common.resilience import (
    ExponentialBackoff,
    KalshiGapTracker,
    SilenceGapTracker,
    silence_watchdog,
    log_gap,
)
from kalshi_ingest import build_auth_headers, load_credentials, WS_PATHS  # noqa: E402
from run_dual_feed import handle_message, DEFAULT_POLYMARKET_TOKENS, DEFAULT_KALSHI_TICKERS  # noqa: E402
from storage.timescale_store import TimescaleStore, dsn_from_env  # noqa: E402
from storage.minio_store import (  # noqa: E402
    client_from_env as minio_client_from_env,
    bucket_from_env as minio_bucket_from_env,
    ensure_bucket as minio_ensure_bucket,
    archive_directory as minio_archive_directory,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

# Fase 4 -- escritura a TimescaleDB por lotes en vez de un INSERT por
# mensaje. Se manda un lote cuando junta STORAGE_BATCH_SIZE eventos O cuando
# pasan STORAGE_FLUSH_INTERVAL_SECONDS desde el último flush, lo que pase
# primero -- así en un mercado tranquilo los eventos no quedan esperando
# en la cola indefinidamente.
STORAGE_BATCH_SIZE = 200
STORAGE_FLUSH_INTERVAL_SECONDS = 2.0
# Si la cola llega a este tamaño (ej: TimescaleDB caído por un buen rato),
# se empiezan a descartar eventos nuevos de la cola en vez de crecer sin
# límite -- el JSONL local YA los tiene a salvo (Fase 1-3), así que esto
# solo implica que el catch-up a la base de datos queda incompleto hasta
# que se reinicie el proceso, nunca pérdida de datos real.
STORAGE_QUEUE_MAXSIZE = 20_000

# Fase 4 -- cada cuánto se sube el contenido de data/ y logs/gaps.jsonl a
# MinIO. No hace falta que sea muy frecuente: es un archivado de respaldo,
# no la ruta primaria de lectura (esa es TimescaleDB para lo normalizado,
# el JSONL local para lo crudo mientras el proceso sigue vivo).
MINIO_ARCHIVE_INTERVAL_SECONDS = 300.0

POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_PING_INTERVAL_SECONDS = 10

# Una conexión se considera "estable" (no un blip transitorio) si se
# mantuvo arriba más de esto -- recién ahí se resetea el backoff a 0.
STABLE_CONNECTION_SECONDS = 30.0
# Timeout de silencio para Polymarket -- ajustable según qué tan activo se
# espera que esté el mercado que estés siguiendo (ver docstring de
# common/resilience.py sobre las limitaciones de este enfoque).
POLYMARKET_SILENCE_TIMEOUT_SECONDS = 30.0


class ChaosKillSwitch:
    """Fuerza el cierre de la conexión una vez, pasados N segundos desde que
    se conectó -- para simular un corte real de forma reproducible (paso 10)
    sin tener que cortar el wifi a mano cada vez que querés probar algo."""

    def __init__(self, kill_after_seconds: float | None):
        self.kill_after_seconds = kill_after_seconds
        self.armed = kill_after_seconds is not None

    async def watch(self, ws, logger, source: str):
        if not self.armed:
            return
        await asyncio.sleep(self.kill_after_seconds)
        self.armed = False  # un solo disparo por proceso
        logger.warning("[%s] CHAOS: cerrando la conexión a propósito para simular un fallo.", source)
        try:
            # 1006 es un código reservado por el protocolo WS para "cierre
            # anormal" -- nunca se puede enviar explícitamente, así que
            # ws.close(code=1006) tira ProtocolError. Usamos 1000 (cierre
            # normal), que sí se puede enviar y de verdad corta la conexión.
            await ws.close(code=1000, reason="chaos test")
        except Exception:
            # Esta tarea corre en segundo plano (create_task, sin await
            # directo) -- si algo acá adentro falla y no lo logueamos
            # explícitamente, asyncio se lo traga en silencio y el chaos
            # test queda pareciendo "no hizo nada". Ya nos pasó una vez.
            logger.exception("[%s] CHAOS: falló al intentar cerrar la conexión.", source)


# Si no llega un PONG en respuesta a nuestros PING en este tiempo, se
# considera la conexión muerta (aunque no haya llegado ningún frame de
# cierre) y se fuerza un cierre para que run_with_reconnect actúe. Cubre el
# caso de una conexión "colgada" silenciosamente (ej: un black-hole de red)
# que ni una excepción ni una salida limpia del "async for" detectarían.
POLYMARKET_PONG_TIMEOUT_SECONDS = 30


def _enqueue_for_storage(storage_queue: asyncio.Queue | None, event: dict, source: str, logger):
    """Empuja un evento normalizado a la cola de storage sin bloquear jamás
    la ingesta (paso central de Fase 3: el feed en tiempo real no puede
    esperar a la base de datos). Si la cola está llena, se descarta el
    evento MÁS NUEVO y se avisa -- ver STORAGE_QUEUE_MAXSIZE."""
    if storage_queue is None:
        return
    try:
        storage_queue.put_nowait(event)
    except asyncio.QueueFull:
        logger.warning(
            "[%s] Cola de storage llena (%d) -- se descarta un evento del catch-up a TimescaleDB "
            "(el JSONL local ya lo tiene a salvo).",
            source, STORAGE_QUEUE_MAXSIZE,
        )


async def run_polymarket_once(tokens: list[str], logger, normalized_sink, chaos: ChaosKillSwitch,
                               storage_queue: asyncio.Queue | None = None):
    subscribe_msg = json.dumps({"assets_ids": tokens, "type": "market"})
    last_pong_at = {"ts": time.monotonic()}

    async def heartbeat(ws):
        try:
            while True:
                await asyncio.sleep(POLYMARKET_PING_INTERVAL_SECONDS)
                await ws.send("PING")
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass

    async def stale_pong_watchdog(ws):
        try:
            while True:
                await asyncio.sleep(5)
                elapsed = time.monotonic() - last_pong_at["ts"]
                if elapsed > POLYMARKET_PONG_TIMEOUT_SECONDS:
                    logger.warning(
                        "[polymarket] Sin PONG hace %.1fs (timeout %ds) -- conexión probablemente muerta, forzando cierre.",
                        elapsed, POLYMARKET_PONG_TIMEOUT_SECONDS,
                    )
                    try:
                        await ws.close(code=1000, reason="stale pong")
                    except Exception:
                        logger.exception("[polymarket] Falló al cerrar la conexión tras detectar PONG vencido.")
                    return
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass

    logger.info("[polymarket] Conectando a %s", POLYMARKET_WS_URL)
    async with websockets.connect(POLYMARKET_WS_URL, ping_interval=None) as ws:
        logger.info("[polymarket] Conectado. Suscribiendo a %d token(s).", len(tokens))
        await ws.send(subscribe_msg)
        last_pong_at["ts"] = time.monotonic()

        hb_task = asyncio.create_task(heartbeat(ws))
        watchdog_task = asyncio.create_task(stale_pong_watchdog(ws))
        chaos_task = asyncio.create_task(chaos.watch(ws, logger, "polymarket"))
        try:
            async for raw_message in ws:
                if raw_message == "PONG":
                    last_pong_at["ts"] = time.monotonic()
                    continue
                events = handle_message("polymarket", raw_message, logger)
                for event in events:
                    normalized_sink.on_message(datetime.now(timezone.utc))
                    _enqueue_for_storage(storage_queue, event, "polymarket", logger)
        finally:
            hb_task.cancel()
            watchdog_task.cancel()
            chaos_task.cancel()


async def run_kalshi_once(tickers: list[str], logger, gap_tracker: KalshiGapTracker, chaos: ChaosKillSwitch,
                           storage_queue: asyncio.Queue | None = None):
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

        chaos_task = asyncio.create_task(chaos.watch(ws, logger, "kalshi"))
        try:
            async for raw_message in ws:
                events = handle_message("kalshi", raw_message, logger)
                for event in events:
                    if event["event_type"] == "book_snapshot":
                        gap_tracker.on_snapshot(event["outcome_id"], event.get("sequence"), event["received_at"])
                    elif event["event_type"] == "book_update" and event.get("update_kind") == "delta_increment":
                        gap_tracker.on_delta(event["outcome_id"], event.get("sequence"), event["received_at"])
                    _enqueue_for_storage(storage_queue, event, "kalshi", logger)
        finally:
            chaos_task.cancel()


async def run_with_reconnect(connect_once, source: str, logger, gap_log):
    """Envoltorio genérico de reconexión con backoff exponencial (paso 7).
    connect_once es una corrutina que corre "para siempre" hasta que la
    conexión se cae; cuando eso pasa, esta función loggea el gap de
    conexión (paso 9), espera el backoff, y reintenta indefinidamente."""
    backoff = ExponentialBackoff(base=1.0, cap=60.0)

    while True:
        connected_at = time.monotonic()
        reason = None
        try:
            await connect_once()
            # connect_once() puede terminar SIN lanzar excepción: la
            # librería websockets sale limpio del "async for" cuando el
            # cierre fue con código normal (1000/1001) -- que es justo lo
            # que hace nuestro propio ChaosKillSwitch, y lo que también
            # puede pasar en un cierre normal iniciado por el servidor.
            # Si tratáramos esto como "no pasó nada", el gap no se
            # loggearía y el backoff nunca se resetearía tras una
            # reconexión larga -- eso fue exactamente el bug que encontró
            # el usuario con --chaos-kill-after-polymarket.
            reason = "cierre limpio (sin excepción)"
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            reason = str(e)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[%s] Error inesperado, se trata igual como una desconexión.", source)
            reason = "excepción inesperada (ver traceback arriba)"

        disconnect_ts = datetime.now(timezone.utc).isoformat()
        uptime = time.monotonic() - connected_at
        if uptime > STABLE_CONNECTION_SECONDS:
            backoff.reset()
        logger.warning("[%s] Conexión perdida tras %.1fs arriba (%s).", source, uptime, reason)

        delay = backoff.next_delay()
        logger.info("[%s] Reintentando en %.1fs (intento #%d).", source, delay, backoff.attempt)
        await asyncio.sleep(delay)

        reconnected_ts = datetime.now(timezone.utc).isoformat()
        log_gap(
            gap_log, source, "connection_gap",
            start_ts=disconnect_ts, end_ts=reconnected_ts,
            detail=f"reconectando (intento #{backoff.attempt}, delay usado {delay:.1f}s, motivo: {reason})",
        )


async def storage_writer_task(storage_queue: asyncio.Queue, store: TimescaleStore, logger):
    """Drena la cola de eventos normalizados y los inserta en TimescaleDB
    por lotes (paso central de Fase 4). Corre "para siempre" hasta que se
    cancela junto con el resto de las tareas al cerrar el proceso."""
    batch = []

    async def flush():
        if not batch:
            return
        try:
            n = await store.insert_events(batch)
            logger.debug("[storage] Insertados %d eventos en TimescaleDB.", n)
        except Exception:
            logger.exception(
                "[storage] Falló el insert por lote a TimescaleDB (%d eventos se pierden del catch-up, "
                "el JSONL local los tiene a salvo igual).", len(batch),
            )
        batch.clear()

    try:
        while True:
            try:
                event = await asyncio.wait_for(storage_queue.get(), timeout=STORAGE_FLUSH_INTERVAL_SECONDS)
                batch.append(event)
            except asyncio.TimeoutError:
                await flush()
                continue

            if len(batch) >= STORAGE_BATCH_SIZE:
                await flush()
    except asyncio.CancelledError:
        await flush()  # último intento de no perder lo que quedó a medio juntar
        raise


async def minio_archiver_task(client, bucket: str, logger):
    """Sube data/*.jsonl y logs/gaps.jsonl a MinIO cada
    MINIO_ARCHIVE_INTERVAL_SECONDS. Un fallo de MinIO nunca frena esta
    tarea ni el resto del pipeline -- ver docstring de storage/minio_store.py."""
    try:
        while True:
            await asyncio.sleep(MINIO_ARCHIVE_INTERVAL_SECONDS)
            n1 = minio_archive_directory(client, bucket, DATA_DIR, "data", logger)
            n2 = minio_archive_directory(client, bucket, LOGS_DIR, "logs", logger, patterns=("gaps.jsonl",))
            logger.info("[minio] Archivado periódico: %d archivo(s) de data/, %d de logs/.", n1, n2)
    except asyncio.CancelledError:
        raise


def _setup_storage(logger):
    """Intenta conectar TimescaleDB y MinIO. Si algo falla, loggea y
    devuelve None para esa pieza -- el resto del pipeline sigue andando sin
    esa parte del storage (ver docstring del módulo)."""
    store = None
    minio_client = None
    minio_bucket = None

    try:
        store = TimescaleStore(dsn_from_env())
    except Exception:
        logger.exception("[storage] No se pudo preparar la conexión a TimescaleDB.")
        store = None

    try:
        minio_client = minio_client_from_env()
        minio_bucket = minio_bucket_from_env()
        minio_ensure_bucket(minio_client, minio_bucket)
    except Exception:
        logger.exception(
            "[storage] No se pudo conectar/preparar el bucket de MinIO -- se sigue sin archivado a MinIO "
            "(¿está corriendo `docker compose up -d`?)."
        )
        minio_client = None
        minio_bucket = None

    return store, minio_client, minio_bucket


async def main_async(polymarket_tokens, kalshi_tickers, logger, gap_log,
                      chaos_kill_after_polymarket, chaos_kill_after_kalshi, storage_enabled: bool):
    silence_tracker = SilenceGapTracker("polymarket", POLYMARKET_SILENCE_TIMEOUT_SECONDS, gap_log)
    kalshi_gap_tracker = KalshiGapTracker(gap_log)

    poly_chaos = ChaosKillSwitch(chaos_kill_after_polymarket)
    kalshi_chaos = ChaosKillSwitch(chaos_kill_after_kalshi)

    watchdog_task = asyncio.create_task(silence_watchdog(silence_tracker))

    storage_queue = None
    store = None
    minio_client = None
    minio_bucket = None
    storage_task = None
    archiver_task = None

    if storage_enabled:
        store, minio_client, minio_bucket = _setup_storage(logger)

        if store is not None:
            try:
                await store.connect()
                logger.info("[storage] Conectado a TimescaleDB.")
                storage_queue = asyncio.Queue(maxsize=STORAGE_QUEUE_MAXSIZE)
                storage_task = asyncio.create_task(storage_writer_task(storage_queue, store, logger))
            except Exception:
                logger.exception(
                    "[storage] No se pudo conectar a TimescaleDB -- se sigue sin escritura a base de datos "
                    "(¿está corriendo `docker compose up -d`?)."
                )
                store = None

        if minio_client is not None:
            archiver_task = asyncio.create_task(minio_archiver_task(minio_client, minio_bucket, logger))
            logger.info("[storage] Archivado periódico a MinIO activado (cada %.0fs).", MINIO_ARCHIVE_INTERVAL_SECONDS)
    else:
        logger.info("[storage] Desactivado por --no-storage. Solo JSONL local (Fase 1-3).")

    async def poly_connect_once():
        await run_polymarket_once(polymarket_tokens, logger, silence_tracker, poly_chaos, storage_queue)

    async def kalshi_connect_once():
        await run_kalshi_once(kalshi_tickers, logger, kalshi_gap_tracker, kalshi_chaos, storage_queue)

    try:
        await asyncio.gather(
            run_with_reconnect(poly_connect_once, "polymarket", logger, gap_log),
            run_with_reconnect(kalshi_connect_once, "kalshi", logger, gap_log),
        )
    finally:
        watchdog_task.cancel()
        if storage_task is not None:
            storage_task.cancel()
        if archiver_task is not None:
            archiver_task.cancel()
        if minio_client is not None:
            # Última subida antes de cerrar, para no depender de que el
            # próximo ciclo periódico llegue a tiempo.
            minio_archive_directory(minio_client, minio_bucket, DATA_DIR, "data", logger)
            minio_archive_directory(minio_client, minio_bucket, LOGS_DIR, "logs", logger, patterns=("gaps.jsonl",))
        if store is not None:
            await store.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Fase 3: ingesta resiliente con reconexión y detección de gaps")
    parser.add_argument("--polymarket-tokens", nargs="+", default=DEFAULT_POLYMARKET_TOKENS)
    parser.add_argument("--kalshi-tickers", nargs="+", default=DEFAULT_KALSHI_TICKERS)
    parser.add_argument(
        "--chaos-kill-after-polymarket", type=float, default=None,
        help="Segundos tras conectar para forzar un corte simulado del feed de Polymarket (una sola vez).",
    )
    parser.add_argument(
        "--chaos-kill-after-kalshi", type=float, default=None,
        help="Segundos tras conectar para forzar un corte simulado del feed de Kalshi (una sola vez).",
    )
    parser.add_argument(
        "--no-storage", action="store_true",
        help="No intentar conectar a TimescaleDB/MinIO (Fase 4). Útil para probar Fase 3 sin Docker levantado.",
    )
    return parser.parse_args()


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    logger = setup_logging("resilient_feed")
    gap_log = get_gap_logger()
    args = parse_args()

    try:
        asyncio.run(main_async(
            args.polymarket_tokens, args.kalshi_tickers, logger, gap_log,
            args.chaos_kill_after_polymarket, args.chaos_kill_after_kalshi,
            storage_enabled=not args.no_storage,
        ))
    except KeyboardInterrupt:
        logger.info("Interrumpido por el usuario, cerrando ambos feeds.")


if __name__ == "__main__":
    main()
