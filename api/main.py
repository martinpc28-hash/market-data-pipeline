"""
Fase 5 — API de consumo (FastAPI).

Expone lo que junta el pipeline de Fase 1-4 de una forma consultable:
  GET /markets                  -- qué outcome_id hay disponibles
  GET /price/{outcome_id}       -- precio actual (best bid/ask/mid), reconstruido
  GET /history/{outcome_id}     -- histórico crudo, paginado
  GET /spread                   -- spread entre plataformas para los pares
                                    confirmados en config/market_mapping.json

Fase 7 (dashboard) agrega:
  GET /system/status            -- salud de TimescaleDB, MinIO y frescura de
                                    cada feed (verde/amarillo/rojo)
  GET /system/gaps              -- últimos gaps detectados (logs/gaps.jsonl)
  GET /dashboard                -- la página del dashboard en sí (static/dashboard.html)

Decisión de diseño importante: "precio actual" NO es un campo que venga
suelto en algún mensaje de los exchanges -- ver el docstring de
common/orderbook.py. Se reconstruye en el momento de cada request,
aplicando el último book_snapshot y sus updates posteriores desde
TimescaleDB. Es una reconstrucción bajo demanda, no un valor cacheado --
para el volumen de este proyecto (unos pocos outcomes trackeados) alcanza
sin necesitar una capa de cache; si el proyecto creciera a muchos mercados
en simultáneo, ahí sí convendría mantener el book en memoria del lado de
la ingesta en vez de reconstruirlo por request (queda anotado como mejora
futura, no hace falta para el objetivo de este proyecto).

Uso:
    uvicorn api.main:app --reload --port 8000
Documentación interactiva autogenerada en http://localhost:8000/docs
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.orderbook import reconstruct_book, best_bid, best_ask, midpoint  # noqa: E402
from storage.timescale_store import TimescaleStore, dsn_from_env  # noqa: E402
from storage.minio_store import (  # noqa: E402
    client_from_env as minio_client_from_env,
    bucket_from_env as minio_bucket_from_env,
)
# Reutiliza el fetch de mercados activos que ya existía para el paso 6
# (tools/market_matcher.py, sugerencia de pares candidatos) -- acá se usa
# para /discover/markets, la búsqueda en vivo del dashboard. NO es una
# nueva integración: es el mismo código, un solo lugar que sabe hablar con
# las APIs REST de Polymarket/Kalshi.
from tools.market_matcher import (  # noqa: E402
    fetch_polymarket_markets,
    fetch_kalshi_markets,
    fetch_trending_polymarket,
    fetch_trending_kalshi,
    fetch_live_polymarket,
    fetch_live_kalshi,
    parse_market_link,
    fetch_polymarket_by_slug,
    fetch_kalshi_by_ticker,
    resolve_polymarket_token_label,
    find_mirror_candidates,
    CATEGORY_KEYWORDS,
)
from common.kalshi_rest import (  # noqa: E402
    KalshiAuthError,
    get_balance as kalshi_get_balance,
    get_positions as kalshi_get_positions,
    get_fills as kalshi_get_fills,
    get_settlements as kalshi_get_settlements,
    settlement_pnl,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MARKET_MAPPING_PATH = PROJECT_ROOT / "config" / "market_mapping.json"
TRACKED_MARKETS_PATH = PROJECT_ROOT / "config" / "tracked_markets.json"
MARKET_LABELS_PATH = PROJECT_ROOT / "config" / "market_labels.json"
GAPS_JSONL_PATH = PROJECT_ROOT / "logs" / "gaps.jsonl"
DASHBOARD_PATH = Path(__file__).resolve().parent / "static" / "dashboard.html"

# Umbrales de "frescura" de un feed para /system/status -- genéricos, no
# atados al timeout de silencio de Polymarket (ese es específico de la
# heurística de gap detection, ver common/resilience.py). Acá es más simple:
# hace cuánto llegó el último evento de cada fuente, para un vistazo de
# infraestructura ("¿está vivo esto ahora mismo?").
STALE_WARNING_SECONDS = 60
STALE_CRITICAL_SECONDS = 300

# /discover/markets (buscador del dashboard) -- ver docstring del endpoint:
# sin esto, una keyword amplia recorre TODO el catálogo activo de la
# plataforma y puede tardar minutos.
DISCOVER_MAX_PAGES = 4
DISCOVER_TIMEOUT_SECONDS = 12.0

# /portfolio/* (panel de Cuentas, Fase 7) -- las llamadas a Kalshi acá
# pueden paginar hasta varias páginas (ver common/kalshi_rest.py), un poco
# más generoso que DISCOVER_TIMEOUT_SECONDS por eso.
PORTFOLIO_TIMEOUT_SECONDS = 20.0

store: TimescaleStore | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store
    store = TimescaleStore(dsn_from_env())
    await store.connect()
    yield
    await store.close()


app = FastAPI(
    title="Market Data Pipeline API",
    description="Precio actual, histórico y spread cross-platform (Polymarket / Kalshi).",
    version="0.1.0",
    lifespan=lifespan,
)


def _dec_to_str(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _load_market_mapping() -> list[dict]:
    if not MARKET_MAPPING_PATH.exists():
        return []
    with MARKET_MAPPING_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("pares", [])


async def _price_for_outcome(outcome_id: str) -> dict:
    """Lógica compartida entre /price y /spread: trae los eventos, arma el
    book, devuelve best bid/ask/mid. Levanta HTTPException 404 si no hay
    ningún dato para ese outcome_id (para /price); /spread la atrapa y
    reporta el faltante en vez de romper la respuesta completa."""
    events = await store.fetch_book_events(outcome_id)
    if not events:
        raise HTTPException(status_code=404, detail=f"No hay datos para outcome_id={outcome_id!r}")

    had_snapshot = events[0].get("_had_snapshot", False)
    book = reconstruct_book(events)
    bid, ask, mid = best_bid(book), best_ask(book), midpoint(book)

    return {
        "outcome_id": outcome_id,
        "best_bid": _dec_to_str(bid),
        "best_ask": _dec_to_str(ask),
        "midpoint": _dec_to_str(mid),
        "as_of": book["as_of"],
        "n_events_used": book["n_events"],
        "reconstruction_had_snapshot": had_snapshot,
        # Si no hubo snapshot de arranque, el book reconstruido puede estar
        # incompleto -- se lo decimos explícito a quien consulta en vez de
        # devolver un precio con una confianza que no corresponde.
        "warning": None if had_snapshot else (
            "No se encontró un book_snapshot previo a este momento para este outcome_id -- "
            "el resultado se calculó solo con los book_update disponibles y puede estar incompleto."
        ),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


def _load_market_labels() -> dict:
    """Nombres legibles para outcome_id (Polymarket) / market_id (Kalshi) --
    ver el comentario en config/market_labels.json. Se completa solo al
    trackear un mercado nuevo desde el buscador (POST /discover/track);
    si el archivo no existe todavía o algún outcome no tiene entrada,
    /markets simplemente no le agrega title/outcome_label y el dashboard
    cae de vuelta al condition_id/token_id/ticker crudo -- nunca rompe."""
    if not MARKET_LABELS_PATH.exists():
        return {"polymarket": {}, "kalshi": {}}
    try:
        data = json.loads(MARKET_LABELS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        data = {}
    data.setdefault("polymarket", {})
    data.setdefault("kalshi", {})
    return data


def _save_market_labels(data: dict) -> None:
    MARKET_LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKET_LABELS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


@app.get("/markets")
async def markets():
    """Lista todos los outcome_id vistos por el pipeline, con la fuente y
    el último evento que le llegó a cada uno -- punto de partida para saber
    qué pedirle a /price o /history. Además de los campos crudos
    (market_id = condition_id en Polymarket / ticker en Kalshi, outcome_id
    = token_id en Polymarket / "ticker:side" en Kalshi -- ver
    common/schema.py), agrega `title` y `outcome_label` legibles cuando
    hay una entrada en config/market_labels.json para ese mercado; si no
    hay, quedan en null y el dashboard cae de vuelta al ID crudo."""
    outcomes = await store.list_outcomes()
    labels = _load_market_labels()

    for o in outcomes:
        if o.get("source") == "polymarket":
            entry = labels["polymarket"].get(o.get("outcome_id") or "")
            o["title"] = entry.get("title") if entry else None
            o["outcome_label"] = entry.get("outcome") if entry else None
        else:
            entry = labels["kalshi"].get(o.get("market_id") or "")
            o["title"] = entry.get("title") if entry else None
            # Kalshi no necesita guardar el outcome en el label -- ya viene
            # codificado en el propio outcome_id ("TICKER:yes"/"TICKER:no",
            # ver common/schema.py), se parsea acá.
            side = (o.get("outcome_id") or "").rsplit(":", 1)
            o["outcome_label"] = side[1] if len(side) == 2 else None

    return {"markets": outcomes}


@app.get("/price/{outcome_id}")
async def price(outcome_id: str):
    """Precio actual (best bid / best ask / punto medio) de un outcome_id,
    reconstruido a partir del último snapshot + sus updates posteriores.
    Para un outcome_id de Kalshi, best_ask va a ser siempre null -- Kalshi
    no manda asks explícitos para un outcome individual (ver docstring de
    common/orderbook.py), no es un error de esta API."""
    return await _price_for_outcome(outcome_id)


@app.get("/history/{outcome_id}")
async def history(
    outcome_id: str,
    start: Optional[datetime] = Query(None, description="ISO 8601, inclusive. Ej: 2026-08-18T00:00:00Z"),
    end: Optional[datetime] = Query(None, description="ISO 8601, inclusive."),
    limit: int = Query(500, ge=1, le=5000, description="Máximo de eventos a devolver (más reciente primero)."),
):
    """Histórico crudo (sin reconstruir el book) de eventos para un
    outcome_id, opcionalmente acotado por rango de tiempo. Pensado para
    graficar o auditar la actividad, no para saber "el precio en el
    instante X" -- para eso conviene /price con datos de ese momento."""
    events = await store.fetch_history(outcome_id, start=start, end=end, limit=limit)
    if not events:
        raise HTTPException(status_code=404, detail=f"No hay datos para outcome_id={outcome_id!r}")
    return {"outcome_id": outcome_id, "count": len(events), "events": events}


@app.get("/spread")
async def spread():
    """Para cada par confirmado en config/market_mapping.json, compara el
    precio (best_bid, ver nota abajo) del lado "Sí ocurre" en cada
    plataforma y devuelve la diferencia. Se usa best_bid (no midpoint) para
    poder comparar como manzanas con manzanas: los outcome_id de Kalshi no
    tienen ask propio (ver common/orderbook.py), así que midpoint no
    existiría para ese lado -- best_bid sí está disponible siempre que
    haya al menos un nivel de compra."""
    pairs = _load_market_mapping()
    if not pairs:
        return {"pairs": [], "note": "config/market_mapping.json no tiene pares cargados todavía."}

    results = []
    for pair in pairs:
        # config/market_mapping.json no usa nombres de campo 100% uniformes
        # entre pares -- el de la Fed usa "..._yes_recorta" (nombre pensado
        # para ESE evento puntual), el de Feijóo usa "..._yes" genérico. Se
        # prueban ambas variantes en vez de forzar un único nombre de campo
        # que le quede raro a la mitad de los pares.
        poly_outcome = pair.get("polymarket_outcome_id_yes_recorta") or pair.get("polymarket_outcome_id_yes")
        kalshi_outcome = pair.get("kalshi_outcome_id_equivalente_a_recorte") or pair.get("kalshi_outcome_id_equivalente_a_si")
        entry = {
            "descripcion": pair.get("descripcion"),
            "polymarket_outcome_id": poly_outcome,
            "kalshi_outcome_id": kalshi_outcome,
            "verificado_el": pair.get("verificado_el"),
        }

        try:
            poly_price = await _price_for_outcome(poly_outcome) if poly_outcome else None
        except HTTPException:
            poly_price = None
        try:
            kalshi_price = await _price_for_outcome(kalshi_outcome) if kalshi_outcome else None
        except HTTPException:
            kalshi_price = None

        entry["polymarket_best_bid"] = poly_price["best_bid"] if poly_price else None
        entry["kalshi_best_bid"] = kalshi_price["best_bid"] if kalshi_price else None

        if poly_price and kalshi_price and poly_price["best_bid"] and kalshi_price["best_bid"]:
            spread_value = Decimal(poly_price["best_bid"]) - Decimal(kalshi_price["best_bid"])
            entry["spread"] = str(spread_value)
        else:
            entry["spread"] = None
            entry["spread_unavailable_reason"] = (
                "Falta precio de uno de los dos lados (ver *_best_bid) -- probablemente todavía no llegó "
                "ningún book_snapshot para ese outcome_id en esta corrida del pipeline."
            )

        results.append(entry)

    return {"pairs": results}


def _seconds_since(iso_ts: str | None) -> float | None:
    if not iso_ts:
        return None
    ts = datetime.fromisoformat(iso_ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts).total_seconds()


def _staleness_status(seconds: float | None) -> str:
    """Traduce 'hace cuánto llegó el último evento' a un semáforo. Sin datos
    nunca ("seconds is None") es 'critical', no 'sin info' -- para el
    backoffice, un feed del que nunca llegó nada es tan urgente como uno
    que se cayó."""
    if seconds is None:
        return "critical"
    if seconds <= STALE_WARNING_SECONDS:
        return "good"
    if seconds <= STALE_CRITICAL_SECONDS:
        return "warning"
    return "critical"


_STATUS_RANK = {"good": 0, "warning": 1, "critical": 2}


def _worst_status(statuses: list[str]) -> str:
    return max(statuses, key=lambda s: _STATUS_RANK.get(s, 2)) if statuses else "critical"


@app.get("/system/status")
async def system_status():
    """Salud de la infraestructura para la pantalla de backoffice: ¿responde
    TimescaleDB?, ¿responde MinIO?, y hace cuánto llegó el último evento de
    cada feed (polymarket/kalshi). Nunca lanza excepción por un chequeo que
    falla -- un componente caído se reporta como 'critical' en la respuesta,
    no tira abajo el endpoint entero (el mismo principio de 'cero pérdida de
    datos silenciosa' aplicado acá: un fallo de infra siempre queda visible,
    nunca provoca un 500 que oculte cuál de los componentes es el problema)."""
    result = {"checked_at": datetime.now(timezone.utc).isoformat()}

    # TimescaleDB
    db_start = time.monotonic()
    try:
        async with store._pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        result["timescaledb"] = {"status": "good", "latency_ms": round((time.monotonic() - db_start) * 1000, 1)}
    except Exception as e:
        result["timescaledb"] = {"status": "critical", "error": str(e)}

    # MinIO -- cliente sync, se corre en un thread aparte para no bloquear
    # el event loop de FastAPI mientras espera la respuesta de red.
    try:
        minio_client = minio_client_from_env()
        bucket = minio_bucket_from_env()
        minio_start = time.monotonic()
        # Timeout corto: si MinIO no responde (caído, DNS lento, etc.) no
        # queremos que /system/status se cuelgue varios segundos -- mejor
        # reportar "critical" rápido que dejar el dashboard congelado.
        exists = await asyncio.wait_for(
            asyncio.to_thread(minio_client.bucket_exists, bucket), timeout=2.5
        )
        result["minio"] = {
            "status": "good" if exists else "warning",
            "latency_ms": round((time.monotonic() - minio_start) * 1000, 1),
            "bucket_exists": exists,
        }
    except asyncio.TimeoutError:
        result["minio"] = {"status": "critical", "error": "timeout (>2.5s) consultando MinIO"}
    except Exception as e:
        result["minio"] = {"status": "critical", "error": str(e)}

    # Frescura por fuente
    sources = {}
    try:
        for m in await store.list_outcomes():
            src = m["source"]
            last_seen = m["last_seen"]
            if src not in sources or (last_seen and last_seen > sources[src]["last_seen"]):
                sources[src] = {"last_seen": last_seen}
    except Exception:
        sources = {}

    source_statuses = []
    for src in ("polymarket", "kalshi"):
        last_seen = sources.get(src, {}).get("last_seen")
        seconds = _seconds_since(last_seen)
        st = _staleness_status(seconds)
        source_statuses.append(st)
        result[src] = {
            "status": st,
            "last_seen": last_seen,
            "seconds_since_last_event": round(seconds, 1) if seconds is not None else None,
        }

    result["overall"] = _worst_status(
        [result["timescaledb"]["status"], result["minio"]["status"], *source_statuses]
    )
    return result


@app.get("/system/gaps")
async def system_gaps(limit: int = Query(50, ge=1, le=1000)):
    """Últimos gaps detectados por la ingesta (logs/gaps.jsonl, escrito por
    common/resilience.py::log_gap). Si el archivo no existe todavía (proceso
    recién arrancado, ningún gap detectado aún) devuelve una lista vacía, no
    un error -- no tener gaps es el caso feliz, no una falla."""
    if not GAPS_JSONL_PATH.exists():
        return {"gaps": [], "count": 0}

    lines = GAPS_JSONL_PATH.read_text(encoding="utf-8").splitlines()
    gaps = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            gaps.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # una línea corrupta no debe tirar abajo el resto

    gaps.reverse()  # más reciente primero
    gaps = gaps[:limit]
    return {"gaps": gaps, "count": len(gaps)}


INGESTION_CONTAINER_NAME = "market_data_ingestion"  # ver container_name en docker-compose.yml


@app.post("/system/restart_ingestion")
async def restart_ingestion():
    """Reinicia el contenedor de ingesta para que tome los mercados nuevos
    agregados vía /discover/track, sin tener que ir a la terminal a correr
    `docker compose restart ingestion` a mano -- botón "🔄 Reiniciar
    ingesta" del dashboard (Cuentas/Backoffice y después de agregar un
    mercado).

    Necesita acceso al socket de Docker del host (montado en
    docker-compose.yml SOLO para el contenedor de la API -- ver el
    comentario ahí). ADVERTENCIA DE SEGURIDAD: el socket de Docker no
    tiene permisos granulares -- cualquier proceso con acceso de
    escritura acá puede controlar CUALQUIER contenedor del host (crear,
    borrar, montar volúmenes arbitrarios), no solo reiniciar este uno.
    Es un tradeoff consciente para un proyecto de portfolio corriendo en
    tu propia máquina; si esto fuera a un servidor compartido, la
    alternativa correcta es un docker-socket-proxy que exponga SOLO la
    acción de restart sobre este contenedor puntual, o un mecanismo de
    recarga sin restart (la ingesta escuchando cambios en
    config/tracked_markets.json en vez de leerlo solo al arrancar) --
    ninguno de los dos implementado acá por simplicidad."""
    try:
        import docker
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail=(
                "Falta el paquete 'docker' en la imagen de la API -- agregalo a "
                "requirements.txt (ya debería estar) y reconstruí con docker compose up -d --build."
            ),
        )

    try:
        client = docker.from_env()
        container = client.containers.get(INGESTION_CONTAINER_NAME)
        container.restart(timeout=10)
    except docker.errors.NotFound:
        raise HTTPException(
            status_code=404,
            detail=f"No encontré el contenedor '{INGESTION_CONTAINER_NAME}' -- ¿está corriendo docker compose aquí mismo?",
        )
    except docker.errors.DockerException as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"No pude hablar con Docker ({e}) -- ¿está montado /var/run/docker.sock en el "
                "contenedor de la API? Ver docker-compose.yml, servicio 'api'."
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"No pude reiniciar la ingesta: {e}")

    return {"restarted": True, "container": INGESTION_CONTAINER_NAME}


@app.get("/dashboard")
async def dashboard():
    """Sirve el dashboard interactivo (Fase 7) -- ver api/static/dashboard.html."""
    if not DASHBOARD_PATH.exists():
        raise HTTPException(status_code=404, detail="Dashboard no encontrado en api/static/dashboard.html")
    return FileResponse(DASHBOARD_PATH)


def _normalize_poly_market(m: dict) -> dict:
    outcomes = m.get("outcomes") or []
    token_ids = m.get("token_ids") or []
    prices = m.get("outcome_prices") or []
    return {
        "source": "polymarket",
        "market_id": m.get("condition_id"),
        "title": m.get("title"),
        "volume_24h": m.get("volume_24h"),
        "best_bid": m.get("best_bid"),
        "best_ask": m.get("best_ask"),
        "outcomes": [
            {"label": lbl, "token_id": tok, "price": prices[i] if i < len(prices) else None}
            for i, (lbl, tok) in enumerate(zip(outcomes, token_ids))
        ],
    }


def _normalize_kalshi_market(m: dict) -> dict:
    return {
        "source": "kalshi",
        "market_id": m.get("ticker"),
        "title": m.get("title"),
        "event_ticker": m.get("event_ticker"),
        "volume_24h": m.get("volume_24h"),
        "yes_bid": m.get("yes_bid"),
        "yes_ask": m.get("yes_ask"),
    }


@app.get("/discover/categories")
async def discover_categories():
    """Categorías disponibles para filtrar /discover/trending, /discover/live
    y /discover/markets -- calcadas de kalshi.com/category/*. Ver el
    comentario sobre CATEGORY_KEYWORDS en tools/market_matcher.py: es un
    filtro por palabra clave en el título, no una categorización real de
    ninguna de las dos plataformas."""
    return {"categories": list(CATEGORY_KEYWORDS.keys())}


def _category_keywords_or_400(category: str | None) -> list[str] | None:
    if not category:
        return None
    if category not in CATEGORY_KEYWORDS:
        raise HTTPException(
            status_code=400,
            detail=f"Categoría desconocida: {category!r}. Ver /discover/categories.",
        )
    return CATEGORY_KEYWORDS[category]


@app.get("/discover/trending")
async def discover_trending(
    source: str = Query("all", pattern="^(all|polymarket|kalshi)$"),
    limit: int = Query(10, ge=1, le=30),
    category: Optional[str] = Query(None, description="Ver /discover/categories"),
    min_volume: Optional[float] = Query(
        None, ge=0, description="Volumen mínimo de 24h -- sube el piso por defecto (> 0)."
    ),
):
    """Mercados con más volumen en las últimas 24h, en vivo -- a diferencia
    de /discover/markets, no hace falta escribir ninguna keyword, y como es
    UNA sola página por fuente (ver fetch_trending_* en
    tools/market_matcher.py) siempre responde rápido, sin el riesgo de
    paginar de más que tenía la búsqueda por texto (ver nota en
    /discover/markets). Solo incluye mercados con volumen > 0 -- ver
    docstring de fetch_trending_kalshi. `category` filtra por keyword antes
    de rankear -- ver CATEGORY_KEYWORDS. `min_volume` sube ese piso (nunca
    lo baja de 0)."""
    keywords = _category_keywords_or_400(category)
    results = []
    try:
        if source in ("all", "polymarket"):
            poly = await asyncio.wait_for(
                asyncio.to_thread(fetch_trending_polymarket, limit, keywords, min_volume), timeout=DISCOVER_TIMEOUT_SECONDS
            )
            results.extend(_normalize_poly_market(m) for m in poly)
        if source in ("all", "kalshi"):
            kal = await asyncio.wait_for(
                asyncio.to_thread(fetch_trending_kalshi, limit, 3, keywords, min_volume), timeout=DISCOVER_TIMEOUT_SECONDS
            )
            results.extend(_normalize_kalshi_market(m) for m in kal)
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"La consulta de tendencias tardó más de {DISCOVER_TIMEOUT_SECONDS}s.",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error consultando la API externa: {e}")

    # Si se pidieron ambas fuentes, se intercalan ordenadas por volumen
    # descendente en vez de mostrar todo Polymarket y después todo Kalshi.
    results.sort(key=lambda r: r.get("volume_24h") or 0, reverse=True)
    return {"count": len(results), "results": results}


@app.get("/discover/live")
async def discover_live(
    source: str = Query("all", pattern="^(all|polymarket|kalshi)$"),
    limit: int = Query(10, ge=1, le=30),
    category: Optional[str] = Query(None, description="Ver /discover/categories"),
    min_volume: Optional[float] = Query(None, ge=0, description="Volumen mínimo de 24h."),
):
    """Mercados abiertos AHORA MISMO, sin exigir volumen -- a diferencia de
    /discover/trending (que solo muestra los que tienen actividad
    reciente), esto muestra todo lo que está corriendo en este momento,
    útil para encontrar mercados recién creados que todavía no
    acumularon volumen. Polymarket ordenado por más nuevo primero; Kalshi
    en el orden que devuelve la API (no soporta ordenar por fecha del
    lado del servidor). `category` -- ver docstring de discover_trending.
    `min_volume`, si se pasa, sí filtra acá (a diferencia del default de
    este endpoint, que no exige volumen)."""
    keywords = _category_keywords_or_400(category)
    results = []
    try:
        if source in ("all", "polymarket"):
            poly = await asyncio.wait_for(
                asyncio.to_thread(fetch_live_polymarket, limit, keywords, min_volume), timeout=DISCOVER_TIMEOUT_SECONDS
            )
            results.extend(_normalize_poly_market(m) for m in poly)
        if source in ("all", "kalshi"):
            kal = await asyncio.wait_for(
                asyncio.to_thread(fetch_live_kalshi, limit, keywords, min_volume), timeout=DISCOVER_TIMEOUT_SECONDS
            )
            results.extend(_normalize_kalshi_market(m) for m in kal)
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"La consulta de mercados en vivo tardó más de {DISCOVER_TIMEOUT_SECONDS}s.",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error consultando la API externa: {e}")

    return {"count": len(results), "results": results}


def _load_tracked_markets() -> dict:
    if not TRACKED_MARKETS_PATH.exists():
        return {"polymarket_tokens": [], "kalshi_tickers": []}
    try:
        data = json.loads(TRACKED_MARKETS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        data = {}
    data.setdefault("polymarket_tokens", [])
    data.setdefault("kalshi_tickers", [])
    return data


@app.get("/discover/markets")
async def discover_markets(
    q: Optional[str] = Query(None, min_length=2, description="Palabra clave a buscar en el título del mercado"),
    source: str = Query("all", pattern="^(all|polymarket|kalshi)$"),
    limit: int = Query(15, ge=1, le=50),
    category: Optional[str] = Query(None, description="Ver /discover/categories"),
    min_volume: Optional[float] = Query(
        None, ge=0, description="Volumen mínimo de 24h en USD (Polymarket) / contratos (Kalshi) -- filtra client-side, ver docstring."
    ),
):
    """Busca mercados ACTIVOS en vivo contra las APIs REST públicas de
    Polymarket/Kalshi -- a diferencia de /markets (que solo lista lo que
    la ingesta ya está trackeando), esto consulta directo a las
    plataformas para encontrar mercados nuevos que todavía no se están
    siguiendo. Ninguna de las dos APIs ofrece búsqueda de texto nativa en
    su endpoint de mercados (Polymarket sí la tiene, pero para eventos/tags,
    no mercados individuales -- ver tools/market_matcher.py), así que se
    trae una página de mercados activos y se filtra acá por keyword.

    Nota importante: ninguna de las dos plataformas soporta filtrar por
    keyword del lado del servidor, así que fetch_polymarket_markets /
    fetch_kalshi_markets paginan y filtran client-side. Sin un tope, una
    keyword amplia ("us", "2028") recorre TODO el catálogo activo de la
    plataforma -- miles de mercados -- y puede tardar minutos (encontrado
    en vivo: una búsqueda de "US 2028" dejó el dashboard "pensando"
    indefinidamente). DISCOVER_MAX_PAGES acota cuántas páginas se piden
    como máximo, y el asyncio.wait_for de abajo es una segunda red de
    seguridad: si aun así se cuelga, el endpoint responde 504 en vez de
    dejar al usuario esperando para siempre.
    Se puede pasar `q` (texto libre), `category` (ver /discover/categories),
    o los dos -- el filtro de título es un OR de todas las keywords
    (las de `q` más las de la categoría), no un AND, así que combinar
    ambos amplía los resultados en vez de acotarlos más. Hace falta al
    menos uno de los dos. `min_volume` es un filtro aparte (AND con lo
    anterior): descarta mercados con menos de ese volumen de 24h -- el
    volumen se lee de la misma página que ya se trajo para el filtro de
    keyword, no cuesta una consulta extra, pero como el volumen tampoco
    se puede pedir ordenado/filtrado del lado del servidor acá (a
    diferencia de /discover/trending), un mercado con volumen alto puede
    seguir sin aparecer si cae fuera de las primeras DISCOVER_MAX_PAGES
    páginas recorridas.
    """
    if not q and not category:
        raise HTTPException(status_code=400, detail="Envía al menos 'q' o 'category'.")

    keywords = q.split() if q else []
    category_keywords = _category_keywords_or_400(category)
    if category_keywords:
        keywords = keywords + category_keywords if keywords else category_keywords

    results = []
    try:
        if source in ("all", "polymarket"):
            poly = await asyncio.wait_for(
                asyncio.to_thread(fetch_polymarket_markets, keywords, 500, DISCOVER_MAX_PAGES, min_volume),
                timeout=DISCOVER_TIMEOUT_SECONDS,
            )
            results.extend(_normalize_poly_market(m) for m in poly[:limit])
        if source in ("all", "kalshi"):
            kal = await asyncio.wait_for(
                asyncio.to_thread(fetch_kalshi_markets, keywords, 200, DISCOVER_MAX_PAGES, min_volume),
                timeout=DISCOVER_TIMEOUT_SECONDS,
            )
            results.extend(_normalize_kalshi_market(m) for m in kal[:limit])
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"La búsqueda tardó más de {DISCOVER_TIMEOUT_SECONDS}s -- prueba una palabra clave más específica.",
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error consultando la API externa: {e}")

    return {
        "query": q,
        "count": len(results),
        "results": results,
        "note": (
            f"La búsqueda solo recorre las primeras {DISCOVER_MAX_PAGES} páginas de mercados "
            "activos por fuente para responder rápido -- si no encuentras lo que buscabas, "
            "prueba una palabra clave más específica."
            + (f" Filtrando por volumen 24h ≥ {min_volume:g}." if min_volume else "")
        ),
    }


@app.get("/discover/resolve_link")
async def discover_resolve_link(url: str = Query(..., min_length=8)):
    """Resuelve un link pegado en el buscador (evento de Polymarket o
    mercado/evento de Kalshi) directo a los mercados reales, en vez de
    tener que adivinar una palabra clave -- ver parse_market_link en
    tools/market_matcher.py para los patrones soportados:
      - https://polymarket.com/event/<slug> (con o sin código de idioma,
        ej. /es/event/<slug>)
      - https://polymarket.com/market/<slug>
      - https://kalshi.com/markets/<series_ticker>/<slug>/<ticker>
    No es infalible: si alguna de las dos plataformas cambia su
    estructura de URLs esto puede dejar de matchear -- en ese caso
    devuelve 400 sugiriendo usar la búsqueda por palabra clave."""
    parsed = parse_market_link(url)
    if not parsed:
        raise HTTPException(
            status_code=400,
            detail=(
                "No reconozco ese link -- prueba pegar un link de evento/mercado de "
                "polymarket.com o kalshi.com, o busca por palabra clave en su lugar."
            ),
        )

    try:
        if parsed["platform"] == "polymarket":
            markets = await asyncio.wait_for(
                asyncio.to_thread(fetch_polymarket_by_slug, parsed["value"], parsed["kind"]),
                timeout=DISCOVER_TIMEOUT_SECONDS,
            )
            results = [_normalize_poly_market(m) for m in markets]
        else:
            markets = await asyncio.wait_for(
                asyncio.to_thread(fetch_kalshi_by_ticker, parsed["value"]),
                timeout=DISCOVER_TIMEOUT_SECONDS,
            )
            results = [_normalize_kalshi_market(m) for m in markets]
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail=f"La consulta tardó más de {DISCOVER_TIMEOUT_SECONDS}s.")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error consultando la API externa: {e}")

    if not results:
        raise HTTPException(
            status_code=404,
            detail=(
                "No encontré ningún mercado para ese link -- puede que ya no exista, "
                "o que la plataforma haya cambiado su estructura de URLs."
            ),
        )

    return {"platform": parsed["platform"], "count": len(results), "results": results}


class TrackRequest(BaseModel):
    source: str
    title: Optional[str] = None
    outcome_label: Optional[str] = None  # ej. "Yes"/"No" -- ver renderDiscoverCard en dashboard.html
    polymarket_token_ids: Optional[list[str]] = None
    kalshi_ticker: Optional[str] = None


@app.post("/discover/track")
async def discover_track(req: TrackRequest):
    """Agrega un mercado encontrado con /discover/markets a
    config/tracked_markets.json -- NO empieza a recibir datos al toque:
    la ingesta (ingestion/run_dual_feed.py) lee este archivo al arrancar,
    así que hace falta `docker compose restart ingestion` para que tome
    el mercado nuevo. Es una limitación consciente (más simple y honesta
    que fingir una suscripción en caliente al WebSocket que en realidad
    no existe todavía). De paso, si vino `title` (la card del buscador
    siempre lo manda), guarda un nombre legible en
    config/market_labels.json -- sin esto, /markets solo tiene el
    condition_id/token_id/ticker crudo, imposible de reconocer."""
    if req.source not in ("polymarket", "kalshi"):
        raise HTTPException(status_code=400, detail="source debe ser 'polymarket' o 'kalshi'")

    data = _load_tracked_markets()
    labels = _load_market_labels()
    added = []
    if req.source == "polymarket":
        if not req.polymarket_token_ids:
            raise HTTPException(status_code=400, detail="Faltan polymarket_token_ids")
        for tok in req.polymarket_token_ids:
            if tok not in data["polymarket_tokens"]:
                data["polymarket_tokens"].append(tok)
                added.append(tok)
            if req.title:
                labels["polymarket"][tok] = {"title": req.title, "outcome": req.outcome_label}
    else:
        if not req.kalshi_ticker:
            raise HTTPException(status_code=400, detail="Falta kalshi_ticker")
        if req.kalshi_ticker not in data["kalshi_tickers"]:
            data["kalshi_tickers"].append(req.kalshi_ticker)
            added.append(req.kalshi_ticker)
        if req.title:
            labels["kalshi"][req.kalshi_ticker] = {"title": req.title}

    data["_comentario"] = (
        "Mercados agregados desde el buscador del dashboard (Fase 7), además "
        "de los defaults hardcodeados en ingestion/run_dual_feed.py. Reinicia "
        "la ingesta (docker compose restart ingestion) después de agregar uno "
        "para que empiece a trackearlo."
    )
    TRACKED_MARKETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    TRACKED_MARKETS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _save_market_labels(labels)

    return {
        "added": added,
        "already_tracked": len(added) == 0,
        "note": "Reinicia la ingesta para que empiece a recibir datos de este mercado: docker compose restart ingestion",
    }


class UntrackRequest(BaseModel):
    source: str
    outcome_id: Optional[str] = None  # Polymarket -- un token_id (un outcome) por vez
    market_id: Optional[str] = None   # Kalshi -- el ticker (borra los dos lados yes/no de una)


@app.post("/discover/untrack")
async def discover_untrack(req: UntrackRequest):
    """Inverso de /discover/track -- saca un mercado de
    config/tracked_markets.json y borra su historial ya guardado en
    TimescaleDB, para "despejar el dashboard" (botón 🗑 de "Mercados
    trackeados"). El borrado de historial es inmediato -- no hace falta
    reiniciar la ingesta para que desaparezca de /markets. PERO si la
    ingesta sigue corriendo con la suscripción vieja (no se reinició
    después de sacarlo de la config), Polymarket/Kalshi le van a seguir
    mandando eventos de ese mercado y va a reaparecer en el próximo
    poll -- por eso conviene reiniciar la ingesta (🔄 Reiniciar ingesta)
    después de borrar uno.

    Si el mercado NO estaba en config/tracked_markets.json (es uno de
    los defaults hardcodeados en ingestion/run_dual_feed.py), igual se
    borra el historial para que desaparezca ahora, pero va a volver a
    aparecer apenas la ingesta reciba el próximo evento -- se avisa en
    la respuesta, no es un error."""
    if req.source not in ("polymarket", "kalshi"):
        raise HTTPException(status_code=400, detail="source debe ser 'polymarket' o 'kalshi'")

    data = _load_tracked_markets()
    labels = _load_market_labels()
    removed_from_config = False

    if req.source == "polymarket":
        if not req.outcome_id:
            raise HTTPException(status_code=400, detail="Falta outcome_id")
        if req.outcome_id in data["polymarket_tokens"]:
            data["polymarket_tokens"].remove(req.outcome_id)
            removed_from_config = True
        labels["polymarket"].pop(req.outcome_id, None)
        events_purged = await store.delete_by_outcome(req.outcome_id)
    else:
        if not req.market_id:
            raise HTTPException(status_code=400, detail="Falta market_id")
        if req.market_id in data["kalshi_tickers"]:
            data["kalshi_tickers"].remove(req.market_id)
            removed_from_config = True
        labels["kalshi"].pop(req.market_id, None)
        events_purged = await store.delete_by_market(req.market_id)

    TRACKED_MARKETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    TRACKED_MARKETS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _save_market_labels(labels)

    if removed_from_config:
        note = (
            "Ya desapareció del dashboard. Presiona \"🔄 Reiniciar ingesta\" para que la ingesta "
            "deje de recibir datos nuevos de este mercado -- si sigue corriendo con la conexión "
            "vieja, puede volver a aparecer solo."
        )
    else:
        note = (
            "No estaba en config/tracked_markets.json -- probablemente es uno de los defaults "
            "hardcodeados en ingestion/run_dual_feed.py. Borré el historial para que desaparezca "
            "ahora, pero va a volver a aparecer apenas llegue el próximo evento. Para sacarlo del "
            "todo, editá DEFAULT_POLYMARKET_TOKENS / DEFAULT_KALSHI_TICKERS en ese archivo."
        )

    return {"removed_from_config": removed_from_config, "events_purged": events_purged, "note": note}


RESOLVE_LABEL_TIMEOUT_SECONDS = 10.0


@app.post("/discover/resolve_missing_labels")
async def resolve_missing_labels():
    """Completa config/market_labels.json para los mercados trackeados que
    todavía no tienen nombre guardado -- pasa cuando un mercado se agregó
    sin pasar por /discover/track (esa es la única vía que guarda el
    nombre automáticamente, ver su docstring), o de antes de que existiera
    ese guardado. Botón "🔎 Resolver nombres faltantes" del dashboard.

    Para Polymarket resuelve vía resolve_polymarket_token_label (pega
    contra Gamma API con el token_id, ver tools/market_matcher.py); para
    Kalshi reutiliza fetch_kalshi_by_ticker (mismo mecanismo que
    /discover/resolve_link). Cada resolución individual tiene timeout
    corto y no rompe las demás si falla -- se reporta cuántas se
    resolvieron y cuántas quedaron sin poder resolver (mercado cerrado,
    borrado, o error de red puntual)."""
    tracked = _load_tracked_markets()
    labels = _load_market_labels()

    missing_poly = [t for t in tracked["polymarket_tokens"] if t not in labels["polymarket"]]
    missing_kalshi = [t for t in tracked["kalshi_tickers"] if t not in labels["kalshi"]]

    resolved, failed = [], []

    for tok in missing_poly:
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(resolve_polymarket_token_label, tok),
                timeout=RESOLVE_LABEL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            result = None
        except Exception:
            result = None
        if result:
            labels["polymarket"][tok] = result
            resolved.append(tok)
        else:
            failed.append(tok)

    for ticker in missing_kalshi:
        try:
            markets = await asyncio.wait_for(
                asyncio.to_thread(fetch_kalshi_by_ticker, ticker),
                timeout=RESOLVE_LABEL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            markets = []
        except Exception:
            markets = []
        if markets and markets[0].get("title"):
            labels["kalshi"][ticker] = {"title": markets[0]["title"]}
            resolved.append(ticker)
        else:
            failed.append(ticker)

    if resolved:
        _save_market_labels(labels)

    note = f"{len(resolved)} nombre(s) completado(s)."
    if failed:
        note += f" {len(failed)} no se pudieron resolver (el mercado puede estar cerrado, borrado, o hubo un error de red puntual -- prueba de nuevo más tarde)."
    if not missing_poly and not missing_kalshi:
        note = "Todos los mercados trackeados ya tenían nombre guardado -- no había nada para resolver."

    return {"resolved": resolved, "failed": failed, "note": note}


# ==================== SUGERENCIAS DE MERCADOS ESPEJO (Fase 8) ====================
#
# El usuario pidió una forma de que /spread deje de depender SOLO de pares
# armados a mano en config/market_mapping.json -- que el dashboard "proponga"
# candidatos posibles de arbitraje cross-platform, y él confirme cuáles son
# de verdad el mismo evento antes de que se agreguen (ver generate_candidates
# / find_mirror_candidates en tools/market_matcher.py, que ya existían como
# herramienta de CLI para esto mismo y acá se exponen "en vivo").

MIRROR_CANDIDATES_TIMEOUT_SECONDS = 30.0


def _load_market_mapping_full() -> dict:
    """Como _load_market_mapping, pero devuelve el dict completo (con
    _comentario/_como_completar) en vez de solo la lista de 'pares' --
    hace falta para reescribir el archivo sin perder esos campos."""
    if not MARKET_MAPPING_PATH.exists():
        return {"pares": []}
    with MARKET_MAPPING_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("pares", [])
    return data


def _save_market_mapping(data: dict) -> None:
    MARKET_MAPPING_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKET_MAPPING_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


@app.get("/discover/mirror_candidates")
async def discover_mirror_candidates(
    min_score: float = Query(
        45.0, ge=0, le=100,
        description=(
            "Similitud mínima de título (0-100, rapidfuzz). Default más bajo que el histórico (60) porque "
            "en la práctica dos mercados del mismo evento pueden tener títulos bastante distintos entre "
            "plataformas (confirmado en vivo con mercados de Bitcoin: 'Will Bitcoin hit $150k by...' vs "
            "'When will bitcoin hit 150k?' da 50.7/100 pese a ser el mismo mercado) -- el filtro de precio "
            "(max_price_spread) es la señal más confiable para descartar falsos positivos, no el texto solo."
        ),
    ),
    min_volume: Optional[float] = Query(None, ge=0, description="Piso de volumen 24h para entrar al pool comparado."),
    limit_per_platform: int = Query(200, ge=20, le=400, description="Cuántos mercados top-volumen de cada plataforma se comparan entre sí."),
    max_price_spread: Optional[float] = Query(
        0.35, ge=0, le=1,
        description=(
            "Descarta candidatos cuyo precio implícito de 'Yes' difiera en más de esto (0-1) entre "
            "plataformas -- un título parecido con precios muy distintos casi siempre es un evento "
            "distinto, no arbitraje real. Envía 0 para desactivar el filtro y ver todo."
        ),
    ),
    q: Optional[str] = Query(
        None, min_length=2,
        description=(
            "Palabras clave (separadas por espacio) para acotar el barrido a un tema puntual -- ej. "
            "'fed election'. Sin esto se compara 'lo más movido' de cada plataforma sin filtro, que en "
            "la práctica puede no superponerse en tema entre Polymarket y Kalshi (ver docstring de "
            "find_mirror_candidates); con keywords se recorre el catálogo activo completo filtrado, más "
            "lento pero mucho más efectivo para encontrar el mercado puntual que buscas."
        ),
    ),
):
    """Barre los mercados con más volumen de Polymarket y Kalshi (sin
    necesitar palabra clave -- ver find_mirror_candidates) y sugiere pares
    candidatos por similitud de título Y precio parecido, para "posible
    arbitraje" cross-platform. Un score de título alto NO garantiza que
    sea el mismo evento (dos mercados distintos pueden tener títulos
    parecidos, o el mismo evento puede tener reglas de resolución
    distintas entre plataformas) -- por eso además del texto se exige que
    el precio implícito de "Yes" no difiera "ridículamente" entre las dos
    plataformas (ver max_price_spread / DEFAULT_MAX_PRICE_SPREAD en
    tools/market_matcher.py): un spread enorme es la señal más confiable
    de que en realidad NO es el mismo evento, más que el texto solo. Esto
    sigue siendo solo una SUGERENCIA: no se agrega nada a
    config/market_mapping.json hasta que el usuario confirma un candidato
    puntual vía POST /discover/confirm_mirror.

    Se descartan acá los candidatos cuyo condition_id o ticker ya están
    en un par confirmado (para no proponer de nuevo lo que ya se
    aceptó o -- indirectamente -- lo que ya se rechazó al no confirmarlo)."""
    effective_max_spread = max_price_spread if max_price_spread else None
    keywords = q.split() if q else None
    effective_timeout = MIRROR_CANDIDATES_TIMEOUT_SECONDS * (2 if keywords else 1)
    try:
        candidates = await asyncio.wait_for(
            asyncio.to_thread(
                find_mirror_candidates, limit_per_platform, min_score, min_volume, 8, 25,
                effective_max_spread, keywords,
            ),
            timeout=effective_timeout,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"El barrido tardó más de {effective_timeout:g}s -- prueba subir min_volume para achicar el pool.",
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error consultando la API externa: {e}")

    existing_pairs = _load_market_mapping()
    known_condition_ids = {p.get("polymarket_market_id") for p in existing_pairs if p.get("polymarket_market_id")}
    known_tickers = {p.get("kalshi_market_ticker") for p in existing_pairs if p.get("kalshi_market_ticker")}

    candidates = [
        c for c in candidates
        if c["polymarket_condition_id"] not in known_condition_ids
        and c["kalshi_ticker"] not in known_tickers
    ]

    return {
        "count": len(candidates),
        "candidates": candidates,
        "note": (
            "Similitud de texto Y de precio entre mercados, no confirmación real de que sea el mismo "
            "evento -- revisa cada uno (reglas de resolución, fecha, banda de valores) antes de "
            "confirmar. Solo se comparan los mercados con más volumen de cada plataforma "
            f"(top {limit_per_platform}), así que un mercado de nicho puede no aparecer aquí."
            + (
                f" Se descartaron los candidatos con spread de precio implícito mayor a {effective_max_spread:g} "
                "(probablemente eventos distintos, no arbitraje real)."
                if effective_max_spread is not None else
                " Filtro de spread de precio desactivado -- puede haber falsos positivos con precios muy distintos."
            )
        ),
    }


class ConfirmMirrorRequest(BaseModel):
    descripcion: Optional[str] = None
    polymarket_condition_id: str
    polymarket_token_ids: list[str]  # en el mismo orden que polymarket_outcomes
    polymarket_outcomes: list[str]
    polymarket_title: str
    kalshi_ticker: str
    kalshi_title: str
    score: Optional[float] = None
    combined_score: Optional[float] = None  # texto + precio, ver generate_candidates
    # ver docstring de generate_candidates en tools/market_matcher.py --
    # True cuando el "Yes" de Polymarket resultó equivalente al "No" de
    # Kalshi (y viceversa), no a su "Yes". Sin esto el mapeo guardado en
    # market_mapping.json quedaría invertido para ese tipo de par.
    kalshi_inverted: bool = False


@app.post("/discover/confirm_mirror")
async def confirm_mirror(req: ConfirmMirrorRequest):
    """Confirma un candidato devuelto por /discover/mirror_candidates:
    1) lo agrega como par nuevo a config/market_mapping.json (mismo
    formato que los pares armados a mano), 2) trackea los 3 outcome_id
    (los 2 de Polymarket + el ticker de Kalshi, que arrastra sus dos
    lados yes/no) igual que /discover/track, para que la próxima ingesta
    empiece a recibir datos de ellos. Requiere reiniciar la ingesta
    después, como cualquier alta de mercado nuevo (ver docstring de
    /discover/track)."""
    outcomes_lower = [o.strip().lower() for o in req.polymarket_outcomes]
    if len(req.polymarket_token_ids) != 2 or len(req.polymarket_outcomes) != 2 or "yes" not in outcomes_lower or "no" not in outcomes_lower:
        raise HTTPException(
            status_code=400,
            detail=(
                "Solo se pueden confirmar mercados binarios Sí/No -- este candidato tiene "
                f"outcomes {req.polymarket_outcomes!r}, que no matchea ese patrón."
            ),
        )
    yes_idx = outcomes_lower.index("yes")
    no_idx = outcomes_lower.index("no")
    poly_yes_token = req.polymarket_token_ids[yes_idx]
    poly_no_token = req.polymarket_token_ids[no_idx]

    mapping = _load_market_mapping_full()
    if any(p.get("polymarket_market_id") == req.polymarket_condition_id for p in mapping["pares"]):
        raise HTTPException(status_code=409, detail="Este par ya está confirmado en config/market_mapping.json.")

    # Orientación normal: Yes de Polymarket <-> Yes de Kalshi. Invertida
    # (ver ConfirmMirrorRequest.kalshi_inverted): Yes de Polymarket <->
    # No de Kalshi -- el matcher la detectó porque daba spread de precio
    # mucho más chico que la orientación directa.
    kalshi_side_for_poly_yes = "no" if req.kalshi_inverted else "yes"
    kalshi_side_for_poly_no = "yes" if req.kalshi_inverted else "no"

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_pair = {
        "descripcion": req.descripcion or f"{req.polymarket_title} (Polymarket)  <->  {req.kalshi_title} (Kalshi)",
        "nota": (
            f"Confirmado desde el panel de sugerencias del dashboard el {today} "
            f"(match combinado: {req.combined_score if req.combined_score is not None else 'n/d'}/100 -- "
            f"texto: {req.score if req.score is not None else 'n/d'}/100). "
            + (
                "El matcher detectó orientación invertida: el 'Yes' de Polymarket corresponde al "
                "'No' de Kalshi (y viceversa) -- ya se guardó así abajo. "
                if req.kalshi_inverted else ""
            )
            + "Verifica igual que las reglas de resolución coincidan de verdad."
        ),
        "polymarket_market_id": req.polymarket_condition_id,
        "polymarket_outcome_id_yes": poly_yes_token,
        "polymarket_outcome_id_no": poly_no_token,
        "kalshi_market_ticker": req.kalshi_ticker,
        "kalshi_outcome_id_equivalente_a_si": f"{req.kalshi_ticker}:{kalshi_side_for_poly_yes}",
        "kalshi_outcome_id_equivalente_a_no": f"{req.kalshi_ticker}:{kalshi_side_for_poly_no}",
        "verificado_el": today,
    }
    mapping["pares"].append(new_pair)
    _save_market_mapping(mapping)

    tracked = _load_tracked_markets()
    labels = _load_market_labels()
    newly_tracked = []
    for tok in (poly_yes_token, poly_no_token):
        if tok not in tracked["polymarket_tokens"]:
            tracked["polymarket_tokens"].append(tok)
            newly_tracked.append(tok)
        labels["polymarket"][tok] = {
            "title": req.polymarket_title,
            "outcome": "Yes" if tok == poly_yes_token else "No",
        }
    if req.kalshi_ticker not in tracked["kalshi_tickers"]:
        tracked["kalshi_tickers"].append(req.kalshi_ticker)
        newly_tracked.append(req.kalshi_ticker)
    labels["kalshi"][req.kalshi_ticker] = {"title": req.kalshi_title}

    TRACKED_MARKETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    TRACKED_MARKETS_PATH.write_text(json.dumps(tracked, ensure_ascii=False, indent=2), encoding="utf-8")
    _save_market_labels(labels)

    return {
        "pair": new_pair,
        "newly_tracked": newly_tracked,
        "note": (
            "Par agregado a config/market_mapping.json y mercados trackeados. Reinicia la ingesta "
            "(🔄 Reiniciar ingesta, o docker compose restart ingestion) para que empiece a recibir "
            "datos -- recién ahí /spread va a dejar de mostrar \"n/d\" para este par."
        ),
    }


# ==================== PORTFOLIO / CUENTAS (Fase 7) ====================
#
# Panel de cuentas: balance, exposición, P&L realizado, historial de
# trades y curva de evolución de P&L. Kalshi está conectado de verdad
# (reutiliza las credenciales REST de common/kalshi_rest.py); Polymarket
# NO tiene una cuenta conectada todavía (no es solo "no implementado":
# Polymarket es on-chain -- el equivalente sería leer posiciones directo
# de una wallet en Polygon vía la CLOB API o el subgraph, un mecanismo de
# auth totalmente distinto al de Kalshi), así que /portfolio/polymarket/summary
# devuelve una forma de ejemplo con `connected: false` para que el
# dashboard pueda mostrar el mismo layout ya armado, listo para cuando
# haya credenciales reales.

def _kalshi_portfolio_error(e: Exception) -> dict:
    if isinstance(e, KalshiAuthError):
        return {"connected": False, "reason": "not_configured", "error": str(e)}
    return {"connected": False, "reason": "api_error", "error": str(e)}


@app.get("/portfolio/kalshi/summary")
async def portfolio_kalshi_summary():
    """Balance + posiciones abiertas + resumen de liquidaciones (P&L
    realizado, win rate, mejor/peor trade). Una sola llamada para no
    obligar al dashboard a hacer 3 requests separados para pintar el
    panel. Nunca tira 500 si Kalshi no está configurado o falla -- eso es
    un estado normal del panel ("no conectado"), no un error del sistema."""
    try:
        balance, positions, settlements = await asyncio.wait_for(
            asyncio.gather(
                asyncio.to_thread(kalshi_get_balance),
                asyncio.to_thread(kalshi_get_positions),
                asyncio.to_thread(kalshi_get_settlements),
            ),
            timeout=PORTFOLIO_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        return _kalshi_portfolio_error(Exception(f"Timeout consultando Kalshi (>{PORTFOLIO_TIMEOUT_SECONDS}s)."))
    except Exception as e:
        return _kalshi_portfolio_error(e)

    total_exposure = sum(float(p.get("market_exposure_dollars") or 0) for p in positions)
    total_open_realized = sum(float(p.get("realized_pnl_dollars") or 0) for p in positions)

    pnls = [settlement_pnl(s) for s in settlements]
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)

    return {
        "connected": True,
        "balance_dollars": float(balance.get("balance_dollars") or (balance.get("balance", 0) / 100)),
        "portfolio_value_dollars": (
            float(balance.get("portfolio_value")) / 100 if balance.get("portfolio_value") is not None else None
        ),
        "open_positions_count": len(positions),
        "total_exposure_dollars": round(total_exposure, 2),
        "open_positions_realized_pnl_dollars": round(total_open_realized, 2),
        "settled": {
            "count": len(settlements),
            "cumulative_pnl_dollars": round(sum(pnls), 2),
            "win_count": wins,
            "loss_count": losses,
            "win_rate": round(wins / len(pnls), 3) if pnls else None,
            "avg_pnl_dollars": round(sum(pnls) / len(pnls), 2) if pnls else None,
            "best_pnl_dollars": round(max(pnls), 2) if pnls else None,
            "worst_pnl_dollars": round(min(pnls), 2) if pnls else None,
        },
    }


@app.get("/portfolio/kalshi/fills")
async def portfolio_kalshi_fills(limit: int = Query(50, ge=1, le=200)):
    """Historial de trades (ejecuciones) de la cuenta de Kalshi conectada,
    más reciente primero."""
    try:
        fills = await asyncio.wait_for(
            asyncio.to_thread(kalshi_get_fills, 100, 3), timeout=PORTFOLIO_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        return _kalshi_portfolio_error(Exception(f"Timeout consultando Kalshi (>{PORTFOLIO_TIMEOUT_SECONDS}s)."))
    except Exception as e:
        return _kalshi_portfolio_error(e)

    out = []
    for f in fills[:limit]:
        side = f.get("outcome_side")
        price = f.get("yes_price_dollars") if side == "yes" else f.get("no_price_dollars")
        out.append({
            "ticker": f.get("ticker"),
            "outcome_side": side,
            "book_side": f.get("book_side"),
            "count": f.get("count_fp"),
            "price_dollars": price,
            "fee_cost_dollars": f.get("fee_cost"),
            "is_taker": f.get("is_taker"),
            "created_time": f.get("created_time"),
        })

    return {"connected": True, "count": len(out), "fills": out}


@app.get("/portfolio/kalshi/pnl_evolution")
async def portfolio_kalshi_pnl_evolution():
    """Curva de P&L acumulado, reconstruida sumando el P&L realizado de
    cada liquidación (settlement) en orden cronológico -- ver
    settlement_pnl() en common/kalshi_rest.py. Se agrupa por día (el
    último acumulado de cada día) para que el gráfico no tenga un punto
    por cada mercado liquidado. Esto reconstruye el pasado real (a
    diferencia de un snapshot de balance, que solo puede arrancar desde
    ahora) porque Kalshi sí guarda el historial completo de
    liquidaciones."""
    try:
        settlements = await asyncio.wait_for(
            asyncio.to_thread(kalshi_get_settlements), timeout=PORTFOLIO_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        return _kalshi_portfolio_error(Exception(f"Timeout consultando Kalshi (>{PORTFOLIO_TIMEOUT_SECONDS}s)."))
    except Exception as e:
        return _kalshi_portfolio_error(e)

    dated = []
    for s in settlements:
        ts = s.get("settled_time")
        if not ts:
            continue
        dated.append((ts, settlement_pnl(s)))
    dated.sort(key=lambda t: t[0])

    points = []
    running = 0.0
    day_seen = {}
    for ts, pnl in dated:
        running += pnl
        day = ts[:10]  # YYYY-MM-DD
        day_seen[day] = running  # se pisa -- nos quedamos con el último del día

    for day in sorted(day_seen.keys()):
        points.append({"date": day, "cumulative_pnl_dollars": round(day_seen[day], 2)})

    return {"connected": True, "points": points}


@app.get("/portfolio/polymarket/summary")
async def portfolio_polymarket_summary():
    """Polymarket todavía no tiene cuenta conectada -- devuelve la MISMA
    forma que /portfolio/kalshi/summary pero con connected=false y
    example=true, para que el dashboard pueda mostrar la tarjeta ya
    armada (con datos de ejemplo bien marcados) en vez de esconderla.
    Cuando se conecte una wallet real, este endpoint se reemplaza por uno
    que hable con la CLOB API / el subgraph de Polymarket -- mecanismo de
    auth distinto al de Kalshi (firma con la wallet, no RSA-PSS)."""
    return {
        "connected": False,
        "reason": "not_configured",
        "example": True,
        "note": "Polymarket no tiene cuenta conectada todavía -- esto es un ejemplo de cómo se va a ver.",
        "balance_dollars": 500.0,
        "portfolio_value_dollars": 612.4,
        "open_positions_count": 3,
        "total_exposure_dollars": 210.0,
        "open_positions_realized_pnl_dollars": 0.0,
        "settled": {
            "count": 12,
            "cumulative_pnl_dollars": 87.3,
            "win_count": 8,
            "loss_count": 4,
            "win_rate": 0.667,
            "avg_pnl_dollars": 7.28,
            "best_pnl_dollars": 42.0,
            "worst_pnl_dollars": -18.5,
        },
    }
