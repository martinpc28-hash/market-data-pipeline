"""
Herramienta de apoyo para el paso 6 del roadmap (mercados equivalentes),
pensada para cuando el proyecto tenga muchos más mercados dando vueltas y
ya no sea viable revisarlos a mano uno por uno.

Importante: esto NO reemplaza el criterio humano. Sugiere candidatos por
similitud de texto (y, para el panel de mercados espejo, también de
precio y algunas señales semánticas puntuales) entre títulos; vos
confirmás cuáles son realmente el mismo evento (como hicimos a mano con
el ejemplo de la Fed, que requería entender la banda de tasas vigente --
ningún matcher de texto llega a eso solo).

Además de la CLI original (uso de mantenimiento, aparte del pipeline en
tiempo real), este módulo expone las funciones que usa api/main.py para
los paneles "Descubrir y agregar mercados" y "Sugerencias de mercados
espejo" del dashboard (Fase 7) -- ver el docstring de cada función y de
find_mirror_candidates más abajo.

Uso CLI:
    python market_matcher.py --keywords fed "interest rate" election --min-score 55

Qué hace la CLI:
    1. Trae mercados activos de Polymarket y Kalshi vía sus APIs REST.
    2. Se queda solo con los que matchean alguna de las --keywords en el
       título (evita comparar todo contra todo, que no escala).
    3. Calcula similitud de texto entre cada par (Polymarket x Kalshi)
       con rapidfuzz.
    4. Guarda los candidatos por encima de --min-score, ordenados por
       score, en un JSON para revisión humana.
    5. NO escribe nada directo a config/market_mapping.json -- ese archivo
       se sigue completando a mano (o vía el panel del dashboard), copiando
       los pares que decidas confirmar de la lista de candidatos.
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from rapidfuzz import fuzz

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data"

POLYMARKET_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"
KALSHI_MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
KALSHI_SERIES_URL = "https://api.elections.kalshi.com/trade-api/v2/series"

# Categorías reales que acepta el parámetro `category` de /series --
# confirmado en vivo contra la API que "Politics" y "Crypto" devuelven
# resultados. El resto son mejor esfuerzo (nombres razonables en inglés);
# una categoría que no exista como valor válido simplemente devuelve una
# lista vacía -- no rompe la búsqueda, solo no aporta nada para esa.
KALSHI_SERIES_CATEGORIES = [
    "Politics", "Elections", "Sports", "Crypto", "Climate", "Economics",
    "Financials", "Companies", "Technology", "Health", "World", "Entertainment",
]

# Cuántas páginas como máximo se recorren al buscar candidatos a mercado
# espejo CON palabras clave (find_mirror_candidates) -- más generoso que
# DISCOVER_MAX_PAGES de api/main.py porque acá se está armando el pool
# comparado entero, no solo un resultado rápido para mostrar en pantalla.
DISCOVER_MAX_PAGES_MIRROR = 6

# Por encima de esto (0-1, fracción del precio implícito de "Yes"), un
# candidato se descarta directo aunque el texto matchee bien -- un spread
# de precio "ridículo" entre plataformas para títulos parecidos es la
# señal más confiable de que en realidad NO es el mismo evento. Bastante
# conservador: por encima de 35 puntos de diferencia, se descarta el
# candidato en vez de proponerlo como sugerencia.
DEFAULT_MAX_PRICE_SPREAD = 0.35

# Piso para marcar un candidato con instrucción de arbitraje concreta --
# por debajo de esto (2 puntos de precio) las fees/slippage reales de
# cada plataforma probablemente se comen el margen, así que no vale la
# pena mostrarlo como oportunidad.
ARBITRAGE_MIN_MARGIN = 0.02


def _kalshi_display_title(market: dict) -> str:
    """En un evento de Kalshi con varios mercados adentro (ej. "quién va a
    ganar la elección" con un mercado Sí/No por candidato), el campo
    `title` es el mismo para TODOS -- la pregunta del evento, no del
    mercado puntual (confirmado contra la API real: 30+ mercados de la
    elección 2028 comparten el mismo `title`). Lo que sí varía por
    mercado es `yes_sub_title` (nombre del candidato/outcome puntual,
    ej. "Andy Beshear") y `subtitle` (categoría/partido, ej. ":: Democratic").
    Sin esto, la lista de resultados muestra N cards idénticas y solo se
    puede distinguir uno de otro por el ticker -- se arma un título
    compuesto "<pregunta> — <candidato>" para que sea legible."""
    title = market.get("title") or ""
    sub = market.get("yes_sub_title")
    if sub and sub not in title:
        return f"{title} — {sub}" if title else sub
    return title


# Categorías del panel "Descubrir mercados" del dashboard (Fase 7), calcadas
# de la barra de navegación de kalshi.com/category/*. NO es un filtro nativo
# de ninguna de las dos APIs -- Kalshi expone categoría a nivel de
# series/evento (no en /markets, y sin filtro de query en /events), y el
# tag_id de categoría "de verdad" de Polymarket no se pudo confirmar de
# forma confiable. Se implementa como el mismo filtro por palabra clave en
# el título que ya usa fetch_polymarket_markets/fetch_kalshi_markets, solo
# que con una lista de keywords armada a mano por categoría en vez de lo
# que escribe el usuario -- es una aproximación honesta, no una
# clasificación real: un mercado puede quedar afuera de su categoría "real"
# si el título no usa ninguna de estas palabras.
CATEGORY_KEYWORDS = {
    "Elections": ["election", "president", "primary", "runoff", "electoral"],
    "Politics": ["politics", "senate", "congress", "governor", "cabinet", "impeach", "policy", "bill", "supreme court"],
    "Sports": ["nfl", "nba", "nhl", "mlb", "soccer", "football", "basketball", "baseball",
               "tennis", "ufc", "boxing", "golf", "hockey", "olympic", "world cup", "champions league"],
    "Culture": ["movie", "oscar", "grammy", "award", "celebrity", "netflix", "box office", "album"],
    "Crypto": ["bitcoin", "ethereum", "crypto", "btc", "eth", "solana", "dogecoin", "stablecoin"],
    "Commodities": ["oil", "gold", "silver", "gas", "wheat", "commodity", "opec", "barrel"],
    "Climate": ["climate", "hurricane", "temperature", "weather", "wildfire", "emissions", "storm"],
    "Economics": ["gdp", "inflation", "unemployment", "recession", "fed", "interest rate", "cpi", "jobs report"],
    "Mentions": ["mention", "say", "says", "tweet", "post"],
    "Finance": ["stock", "s&p", "nasdaq", "dow jones", "ipo", "earnings", "market cap", "shares"],
    "Tech & Science": ["ai", "tech", "science", "spacex", "nasa", "openai", "apple", "google", "artificial intelligence"],
}


def _normalize_poly_outcome_fields(market: dict) -> tuple[list, list, list]:
    """Polymarket a veces devuelve outcomes/clobTokenIds/outcomePrices como
    JSON-string en vez de lista real, dependiendo del endpoint -- normaliza
    los tres campos acá para no repetir esto en cada fetch_*."""
    outcomes = market.get("outcomes")
    token_ids = market.get("clobTokenIds")
    outcome_prices = market.get("outcomePrices")
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes) if outcomes else []
    if isinstance(token_ids, str):
        token_ids = json.loads(token_ids) if token_ids else []
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices) if outcome_prices else []
    return outcomes or [], token_ids or [], outcome_prices or []


def fetch_polymarket_markets(
    keywords: list[str], page_limit: int = 500, max_pages: int | None = None,
    min_volume: float | None = None,
) -> list[dict]:
    """Trae mercados activos de Polymarket, paginando, y se queda solo con
    los que mencionan alguna keyword en el título -- evita traer y comparar
    contra miles de mercados irrelevantes.

    `max_pages` limita cuántas páginas se recorren como máximo (None =
    sin límite, recorre todo el catálogo activo). Sin límite, una keyword
    amplia puede tardar minutos en devolver -- el matcher offline (uso por
    CLI, este módulo corrido standalone) puede bancarse eso, pero el
    buscador interactivo del dashboard (api/main.py: /discover/markets)
    necesita una cota dura para no quedarse "pensando" indefinidamente.

    `min_volume` filtra por volumen de 24h (client-side, la API no lo
    soporta como filtro de query) -- útil para sacarse de encima mercados
    con volumen 0 o casi nulo, igual que ya hacían fetch_trending_*."""
    results = []
    offset = 0
    pages_fetched = 0
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE) if keywords else None

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            break
        resp = requests.get(
            POLYMARKET_MARKETS_URL,
            params={"active": "true", "closed": "false", "limit": page_limit, "offset": offset},
            timeout=15,
        )
        resp.raise_for_status()
        page = resp.json()
        pages_fetched += 1
        if not page:
            break

        for market in page:
            question = market.get("question", "")
            if pattern and not pattern.search(question):
                continue

            volume = market.get("volume24hr") or market.get("volume") or 0
            volume = float(volume) if volume else 0.0
            if min_volume is not None and volume < min_volume:
                continue

            outcomes, token_ids, outcome_prices = _normalize_poly_outcome_fields(market)
            results.append({
                "platform": "polymarket",
                "title": question,
                "condition_id": market.get("conditionId") or market.get("condition_id"),
                "outcomes": outcomes,
                "token_ids": token_ids,
                "outcome_prices": outcome_prices,
                "volume_24h": volume,
                "best_bid": market.get("bestBid"),
                "best_ask": market.get("bestAsk"),
            })

        offset += page_limit
        if len(page) < page_limit:
            break

    return results


def fetch_kalshi_markets(
    keywords: list[str], page_limit: int = 200, max_pages: int | None = None,
    min_volume: float | None = None,
) -> list[dict]:
    """Trae mercados abiertos de Kalshi, paginando con el cursor que
    devuelve la API, filtrando por keyword en el título igual que con
    Polymarket. Mismos parámetros `max_pages`/`min_volume` que
    fetch_polymarket_markets, mismo motivo."""
    results = []
    cursor = None
    pages_fetched = 0
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE) if keywords else None

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            break
        params = {"status": "open", "limit": page_limit}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(KALSHI_MARKETS_URL, params=params, timeout=15)
        resp.raise_for_status()
        body = resp.json()
        markets = body.get("markets") or []
        pages_fetched += 1

        for market in markets:
            title = _kalshi_display_title(market)
            if pattern and not pattern.search(title):
                continue

            volume = market.get("volume_24h") or market.get("volume") or 0
            volume = float(volume) if volume else 0.0
            if min_volume is not None and volume < min_volume:
                continue

            results.append({
                "platform": "kalshi",
                "title": title,
                "ticker": market.get("ticker"),
                "event_ticker": market.get("event_ticker"),
                "volume_24h": volume,
                "yes_bid": market.get("yes_bid"),
                "yes_ask": market.get("yes_ask"),
            })

        cursor = body.get("cursor")
        if not cursor or not markets:
            break

    return results


def fetch_kalshi_markets_via_series(keywords: list[str], min_volume: float | None = None) -> list[dict]:
    """Alternativa a fetch_kalshi_markets para keywords de nicho -- ver
    docstring de find_mirror_candidates. Confirmado en vivo: paginar
    /markets por keyword puede necesitar recorrer miles de mercados antes
    de llegar a un tema como "bitcoin" (Kalshi tiene una cantidad enorme
    de props deportivos puntuales que dominan el catálogo sin filtrar).
    /series?category=X devuelve un catálogo mucho más chico -- decenas o
    cientos de series por categoría, no mercados individuales -- así que
    se filtra por keyword ahí primero, y recién después se piden los
    mercados de las series que matchearon vía /markets?series_ticker=X
    (rápido y acotado, en vez de paginar todo el catálogo a ciegas).

    No reemplaza a fetch_kalshi_markets, la complementa: si alguna
    categoría no está bien nombrada acá (ver KALSHI_SERIES_CATEGORIES,
    mejor esfuerzo) o el tema no encaja en ninguna categoría de Kalshi,
    esto simplemente no aporta nada -- no falla, no rompe la búsqueda."""
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE)
    matched_tickers = []
    seen_tickers = set()

    for category in KALSHI_SERIES_CATEGORIES:
        try:
            resp = requests.get(KALSHI_SERIES_URL, params={"category": category}, timeout=10)
            resp.raise_for_status()
            series_list = resp.json().get("series") or []
        except requests.RequestException:
            continue
        for s in series_list:
            ticker = s.get("ticker")
            title = s.get("title", "")
            if ticker and ticker not in seen_tickers and pattern.search(title):
                seen_tickers.add(ticker)
                matched_tickers.append(ticker)

    results = []
    for ticker in matched_tickers:
        try:
            resp = requests.get(
                KALSHI_MARKETS_URL,
                params={"series_ticker": ticker, "status": "open", "limit": 200},
                timeout=10,
            )
            resp.raise_for_status()
            # Mismo cuidado que en fetch_polymarket_markets_via_tags: .get
            # con default solo cubre la clave ausente, no un valor null
            # explícito -- `or []` cubre ambos.
            markets = resp.json().get("markets") or []
        except requests.RequestException:
            continue

        for market in markets:
            volume = (
                market.get("volume_24h")
                or market.get("volume_24h_fp")
                or market.get("volume")
                or market.get("volume_fp")
                or 0
            )
            volume = float(volume) if volume else 0.0
            if min_volume is not None and volume < min_volume:
                continue

            results.append({
                "platform": "kalshi",
                "title": _kalshi_display_title(market),
                "ticker": market.get("ticker"),
                "event_ticker": market.get("event_ticker"),
                "volume_24h": volume,
                # /markets?series_ticker=X confirmado en vivo que devuelve
                # yes_bid_dollars/yes_ask_dollars (dólares), NO yes_bid/
                # yes_ask en centavos como el listado general -- mismo caso
                # que fetch_kalshi_by_ticker, ver _kalshi_cents.
                "yes_bid": _kalshi_cents(market, "yes_bid"),
                "yes_ask": _kalshi_cents(market, "yes_ask"),
            })

    return results


def fetch_trending_polymarket(
    limit: int = 100, keywords: list[str] | None = None, min_volume: float | None = None,
) -> list[dict]:
    """Mercados de Polymarket con más volumen de 24h, en vivo -- una sola
    página (grande) ordenada por volumen, no hace falta paginar como en
    fetch_polymarket_markets porque acá el objetivo es "lo más movido",
    no "todo lo que matchea la keyword". Solo incluye mercados con
    volumen > 0 -- un mercado sin actividad no es "trending" aunque
    matchee la keyword. `min_volume`, si se pasa, sube ese piso (nunca lo
    baja de 0)."""
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE) if keywords else None
    floor = max(min_volume or 0.0, 0.0)

    resp = requests.get(
        POLYMARKET_MARKETS_URL,
        params={
            "active": "true", "closed": "false", "limit": 500,
            "order": "volume24hr", "ascending": "false",
        },
        timeout=15,
    )
    resp.raise_for_status()
    page = resp.json() or []

    results = []
    for market in page:
        question = market.get("question", "")
        if pattern and not pattern.search(question):
            continue

        volume = market.get("volume24hr") or market.get("volume") or 0
        volume = float(volume) if volume else 0.0
        if volume <= 0 or volume < floor:
            continue

        outcomes, token_ids, outcome_prices = _normalize_poly_outcome_fields(market)
        results.append({
            "platform": "polymarket",
            "title": question,
            "condition_id": market.get("conditionId") or market.get("condition_id"),
            "outcomes": outcomes,
            "token_ids": token_ids,
            "outcome_prices": outcome_prices,
            "volume_24h": volume,
            "best_bid": market.get("bestBid"),
            "best_ask": market.get("bestAsk"),
        })

    results.sort(key=lambda m: m["volume_24h"], reverse=True)
    return results[:limit]


def fetch_trending_kalshi(
    limit: int = 100, max_pages: int = 3, keywords: list[str] | None = None,
    min_volume: float | None = None,
) -> list[dict]:
    """Análogo a fetch_trending_polymarket para Kalshi -- la API no soporta
    ordenar por volumen del lado del servidor, así que se recorren unas
    pocas páginas (`max_pages`, chico a propósito para responder rápido),
    se ordena client-side por volumen descendente, y se corta a `limit`.
    Igual que en Polymarket, solo entran mercados con volumen > 0."""
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE) if keywords else None
    floor = max(min_volume or 0.0, 0.0)

    results = []
    cursor = None
    pages_fetched = 0
    while pages_fetched < max_pages:
        params = {"status": "open", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(KALSHI_MARKETS_URL, params=params, timeout=15)
        resp.raise_for_status()
        body = resp.json()
        markets = body.get("markets") or []
        pages_fetched += 1

        for market in markets:
            title = _kalshi_display_title(market)
            if pattern and not pattern.search(title):
                continue

            volume = market.get("volume_24h") or market.get("volume") or 0
            volume = float(volume) if volume else 0.0
            if volume <= 0 or volume < floor:
                continue

            results.append({
                "platform": "kalshi",
                "title": title,
                "ticker": market.get("ticker"),
                "event_ticker": market.get("event_ticker"),
                "volume_24h": volume,
                "yes_bid": market.get("yes_bid"),
                "yes_ask": market.get("yes_ask"),
            })

        cursor = body.get("cursor")
        if not cursor or not markets:
            break

    results.sort(key=lambda m: m["volume_24h"], reverse=True)
    return results[:limit]


def fetch_live_polymarket(
    limit: int = 100, keywords: list[str] | None = None, min_volume: float | None = None,
) -> list[dict]:
    """Mercados de Polymarket abiertos AHORA MISMO, ordenados por más
    nuevo primero -- a diferencia de fetch_trending_polymarket, NO exige
    volumen > 0 por defecto (útil para encontrar mercados recién creados
    que todavía no acumularon actividad). `min_volume`, si se pasa, sí
    filtra acá."""
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE) if keywords else None

    resp = requests.get(
        POLYMARKET_MARKETS_URL,
        params={
            "active": "true", "closed": "false", "limit": 500,
            "order": "startDate", "ascending": "false",
        },
        timeout=15,
    )
    resp.raise_for_status()
    page = resp.json() or []

    results = []
    for market in page:
        question = market.get("question", "")
        if pattern and not pattern.search(question):
            continue

        volume = market.get("volume24hr") or market.get("volume") or 0
        volume = float(volume) if volume else 0.0
        if min_volume is not None and volume < min_volume:
            continue

        outcomes, token_ids, outcome_prices = _normalize_poly_outcome_fields(market)
        results.append({
            "platform": "polymarket",
            "title": question,
            "condition_id": market.get("conditionId") or market.get("condition_id"),
            "outcomes": outcomes,
            "token_ids": token_ids,
            "outcome_prices": outcome_prices,
            "volume_24h": volume,
            "best_bid": market.get("bestBid"),
            "best_ask": market.get("bestAsk"),
        })
        if len(results) >= limit:
            break

    return results[:limit]


def fetch_live_kalshi(
    limit: int = 100, keywords: list[str] | None = None, min_volume: float | None = None,
) -> list[dict]:
    """Análogo a fetch_live_polymarket para Kalshi -- la API no soporta
    ordenar por fecha del lado del servidor, así que se devuelve en el
    orden que la API entrega (no hay forma confiable de pedir "más
    nuevo primero"). No exige volumen > 0 por defecto."""
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE) if keywords else None

    resp = requests.get(KALSHI_MARKETS_URL, params={"status": "open", "limit": 200}, timeout=15)
    resp.raise_for_status()
    body = resp.json()
    markets = body.get("markets") or []

    results = []
    for market in markets:
        title = _kalshi_display_title(market)
        if pattern and not pattern.search(title):
            continue

        volume = market.get("volume_24h") or market.get("volume") or 0
        volume = float(volume) if volume else 0.0
        if min_volume is not None and volume < min_volume:
            continue

        results.append({
            "platform": "kalshi",
            "title": title,
            "ticker": market.get("ticker"),
            "event_ticker": market.get("event_ticker"),
            "volume_24h": volume,
            "yes_bid": market.get("yes_bid"),
            "yes_ask": market.get("yes_ask"),
        })
        if len(results) >= limit:
            break

    return results[:limit]


def fetch_polymarket_markets_via_tags(keywords: list[str], min_volume: float | None = None) -> list[dict]:
    """Ruta rápida análoga a fetch_kalshi_markets_via_series (ver su
    docstring): paginar TODO /markets buscando la keyword en el título
    puede necesitar recorrer miles de mercados y no alcanzar a un tema de
    nicho a tiempo. Polymarket organiza sus eventos por tags de una sola
    palabra en minúscula (ej. "bitcoin", "fed", "elections") -- confirmado
    en vivo que /events?tag_slug=<keyword> devuelve directo los eventos de
    ese tema, mucho más chico y rápido que paginar el catálogo entero.

    No reemplaza a fetch_polymarket_markets, la complementa: si ninguna
    keyword coincide con un tag_slug válido, esto no aporta nada -- no
    falla, simplemente no encuentra nada (Polymarket devuelve lista
    vacía para un tag_slug que no existe, no error)."""
    results = []
    seen_condition_ids = set()

    for kw in keywords:
        slug = kw.strip().lower()
        if not slug:
            continue
        try:
            resp = requests.get(
                POLYMARKET_EVENTS_URL,
                params={"tag_slug": slug, "closed": "false", "limit": 100},
                timeout=10,
            )
            resp.raise_for_status()
            events = resp.json()
        except requests.RequestException:
            continue
        if not isinstance(events, list):
            continue

        for event in events:
            # OJO: .get("markets", []) NO alcanza -- ese default solo entra
            # si falta la clave. Confirmado en vivo que algunos eventos
            # devuelven "markets": null (no ausente, sino null explícito),
            # y ahí .get("markets", []) devuelve None igual, no la lista
            # vacía -- "for market in None" revienta con "'NoneType' object
            # is not iterable". `or []` cubre los dos casos.
            for market in event.get("markets") or []:
                # El tag_slug puede traer eventos con mercados individuales
                # ya cerrados/resueltos (ver ejemplo real: un market con
                # "active": true pero "closed": true) -- closed manda.
                if market.get("closed"):
                    continue

                condition_id = market.get("conditionId") or market.get("condition_id")
                if not condition_id or condition_id in seen_condition_ids:
                    continue

                outcomes, token_ids, outcome_prices = _normalize_poly_outcome_fields(market)

                volume = market.get("volumeNum") or market.get("volume") or 0
                volume = float(volume) if volume else 0.0
                if min_volume is not None and volume < min_volume:
                    continue

                seen_condition_ids.add(condition_id)
                results.append({
                    "platform": "polymarket",
                    "title": market.get("question", ""),
                    "condition_id": condition_id,
                    "outcomes": outcomes,
                    "token_ids": token_ids,
                    "outcome_prices": outcome_prices,
                    "volume_24h": volume,
                    "best_bid": market.get("bestBid"),
                    "best_ask": market.get("bestAsk"),
                })

    return results


# ------------------- link pegado / resolución de nombres -------------------

_POLYMARKET_LINK_RE = re.compile(
    r"polymarket\.com/(?:[a-z]{2}/)?(event|market)/([a-z0-9-]+)", re.IGNORECASE
)
_KALSHI_LINK_RE = re.compile(
    r"kalshi\.com/markets/([a-z0-9-]+)/([a-z0-9-]+)(?:/([a-z0-9-]+))?", re.IGNORECASE
)


def parse_market_link(url: str) -> dict | None:
    """Reconoce un link de Polymarket o Kalshi pegado por el usuario en el
    buscador del dashboard y lo traduce a algo que fetch_polymarket_by_slug
    / fetch_kalshi_by_ticker puedan resolver directo, sin depender de que
    el usuario adivine la keyword correcta. Patrones soportados:
      - https://polymarket.com/event/<slug> (con o sin código de idioma,
        ej. /es/event/<slug>)
      - https://polymarket.com/market/<slug>
      - https://kalshi.com/markets/<series_ticker>/<slug>/<ticker>
    Devuelve None si el link no matchea ninguno de los dos formatos --
    quien llama debe pedirle al usuario que use la búsqueda por palabra
    clave en su lugar, no asumir que esto es infalible."""
    m = _POLYMARKET_LINK_RE.search(url)
    if m:
        kind, slug = m.group(1).lower(), m.group(2)
        return {"platform": "polymarket", "kind": kind, "value": slug}

    m = _KALSHI_LINK_RE.search(url)
    if m:
        series_ticker, _slug, ticker = m.group(1), m.group(2), m.group(3)
        # Un link de evento (sin ticker de mercado puntual al final) trae
        # solo el series_ticker -- se resuelve igual vía fetch_kalshi_by_ticker,
        # que ya sabe fallback a "es un event_ticker, no un market ticker".
        value = ticker.upper() if ticker else series_ticker.upper()
        return {"platform": "kalshi", "kind": "market" if ticker else "event", "value": value}

    return None


def fetch_polymarket_by_slug(slug: str, kind: str = "event") -> list[dict]:
    """Resuelve un slug de Polymarket (sacado de un link pegado por el
    usuario, ver parse_market_link) directo a la lista de mercados reales
    -- un evento puede tener varios mercados adentro (ej. "quién gana la
    elección" con un mercado Sí/No por candidato), un "market" es un solo
    mercado puntual."""
    url = POLYMARKET_EVENTS_URL if kind == "event" else POLYMARKET_MARKETS_URL
    resp = requests.get(url, params={"slug": slug}, timeout=15)
    resp.raise_for_status()
    body = resp.json()

    if kind == "event":
        events = body if isinstance(body, list) else [body]
        raw_markets = []
        for event in events or []:
            raw_markets.extend(event.get("markets") or [])
    else:
        raw_markets = body if isinstance(body, list) else [body]

    results = []
    for market in raw_markets:
        if not market:
            continue
        outcomes, token_ids, outcome_prices = _normalize_poly_outcome_fields(market)
        volume = market.get("volumeNum") or market.get("volume") or market.get("volume24hr") or 0
        volume = float(volume) if volume else 0.0
        results.append({
            "platform": "polymarket",
            "title": market.get("question", ""),
            "condition_id": market.get("conditionId") or market.get("condition_id"),
            "outcomes": outcomes,
            "token_ids": token_ids,
            "outcome_prices": outcome_prices,
            "volume_24h": volume,
            "best_bid": market.get("bestBid"),
            "best_ask": market.get("bestAsk"),
        })
    return results


def fetch_kalshi_by_ticker(ticker: str) -> list[dict]:
    """Resuelve un ticker de Kalshi (de mercado puntual o de evento, no
    siempre se puede distinguir a simple vista desde un link) a la lista
    de mercados reales. Primero prueba como ticker de mercado puntual
    (GET /markets/<ticker>); si no existe, reintenta como event_ticker
    (GET /markets?event_ticker=<ticker>), que puede traer varios mercados
    (ej. un evento con un Sí/No por candidato)."""
    try:
        resp = requests.get(f"{KALSHI_MARKETS_URL}/{ticker}", timeout=15)
        if resp.status_code == 200:
            body = resp.json()
            market = body.get("market") or body
            if market and market.get("ticker"):
                return [{
                    "platform": "kalshi",
                    "title": _kalshi_display_title(market),
                    "ticker": market.get("ticker"),
                    "event_ticker": market.get("event_ticker"),
                    "volume_24h": float(market.get("volume_24h") or market.get("volume") or 0),
                    "yes_bid": _kalshi_cents(market, "yes_bid"),
                    "yes_ask": _kalshi_cents(market, "yes_ask"),
                }]
    except requests.RequestException:
        pass

    try:
        resp = requests.get(KALSHI_MARKETS_URL, params={"event_ticker": ticker, "limit": 200}, timeout=15)
        resp.raise_for_status()
        markets_raw = resp.json().get("markets") or []
    except requests.RequestException:
        return []

    results = []
    for market in markets_raw:
        results.append({
            "platform": "kalshi",
            "title": _kalshi_display_title(market),
            "ticker": market.get("ticker"),
            "event_ticker": market.get("event_ticker"),
            "volume_24h": float(market.get("volume_24h") or market.get("volume") or 0),
            "yes_bid": _kalshi_cents(market, "yes_bid"),
            "yes_ask": _kalshi_cents(market, "yes_ask"),
        })
    return results


def resolve_polymarket_token_label(token_id: str) -> str | None:
    """Dado un token_id de Polymarket (los que se guardan en
    config/tracked_markets.json), devuelve un nombre legible para mostrar
    en la tabla del dashboard -- "<pregunta del mercado> — <outcome>" (ej.
    "¿Gana el equipo local? — Yes"), o None si no se pudo resolver
    (mercado cerrado/eliminado, o error de red puntual -- quien llama
    decide qué hacer, esto no reintenta)."""
    try:
        resp = requests.get(POLYMARKET_MARKETS_URL, params={"clob_token_ids": token_id}, timeout=10)
        resp.raise_for_status()
        body = resp.json()
    except requests.RequestException:
        return None

    markets = body if isinstance(body, list) else [body]
    for market in markets or []:
        if not market:
            continue
        outcomes, token_ids, _ = _normalize_poly_outcome_fields(market)
        if token_id not in token_ids:
            continue
        idx = token_ids.index(token_id)
        outcome_label = outcomes[idx] if idx < len(outcomes) else None
        question = market.get("question", "")
        if outcome_label and question:
            return f"{question} — {outcome_label}"
        return question or outcome_label
    return None


def _kalshi_cents(market: dict, field: str) -> int | float | None:
    """Kalshi no es consistente entre endpoints: el listado general
    (/markets) devuelve `yes_bid`/`yes_ask` en centavos (int 0-100), pero
    /markets?series_ticker=X (usado por fetch_kalshi_markets_via_series y
    fetch_kalshi_by_ticker) devuelve `yes_bid_dollars`/`yes_ask_dollars`
    (string en dólares, ej. "0.42") en su lugar -- confirmado en vivo.
    Esto normaliza a centavos sin importar cuál de los dos vino."""
    dollars_key = f"{field}_dollars"
    if market.get(dollars_key) is not None:
        try:
            return round(float(market[dollars_key]) * 100)
        except (TypeError, ValueError):
            pass
    return market.get(field)


def _implied_yes_price_polymarket(pm: dict) -> float | None:
    """Precio implícito (0-1) de que el outcome "Yes" resuelva positivo,
    leído de outcome_prices -- busca "Yes" por nombre en vez de asumir
    que siempre es el índice 0 (el orden de outcomes no está garantizado
    igual entre mercados)."""
    outcomes = pm.get("outcomes") or []
    prices = pm.get("outcome_prices") or []
    for i, label in enumerate(outcomes):
        if str(label).strip().lower() == "yes" and i < len(prices):
            try:
                return float(prices[i])
            except (TypeError, ValueError):
                return None
    return None


def _implied_yes_price_kalshi(km: dict) -> float | None:
    """Precio implícito (0-1) de "Yes" en Kalshi -- punto medio entre
    yes_bid/yes_ask (centavos 0-100), la mejor estimación disponible sin
    acceso al order book completo."""
    bid, ask = km.get("yes_bid"), km.get("yes_ask")
    if bid is None or ask is None:
        return None
    try:
        return round((float(bid) + float(ask)) / 2.0 / 100.0, 4)
    except (TypeError, ValueError):
        return None


# ------------------- señales semánticas (sin LLM, ver charla con el usuario) -------------------
#
# El usuario pidió explícitamente NO usar un LLM para esto (más simple, sin
# costo por búsqueda) -- "relacionar preguntas" se resuelve acá con reglas
# concretas: montos/umbrales mencionados en el título, dirección semántica
# (sube vs. baja), y mes/año de resolución. Ninguna de las tres reemplaza
# el criterio humano, son señales adicionales al texto+precio, visibles en
# semantic_flags para que el usuario decida con esa info.

# Alternativas ordenadas por longitud descendente -- "b" antes que "bps"
# cortaría "25 bps" a "25 b" (parseado como 25 mil millones) porque el
# regex prueba las alternativas en orden y se queda con la primera que
# matchea, no con la más larga. Confirmado con un test dedicado.
_NUMBER_RE = re.compile(r"\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bps|mm|bn|k|m|b|%)?\b", re.IGNORECASE)

_NUMBER_MULTIPLIERS = {
    "k": 1_000, "m": 1_000_000, "mm": 1_000_000, "bn": 1_000_000_000,
    "b": 1_000_000_000, "%": 1, "bps": 1,
}


def _extract_numbers(text: str) -> set[float]:
    """Extrae montos/umbrales numéricos mencionados en un título (ej.
    "$150k" -> {150000.0}, "25 bps" -> {25.0}) -- pensado para detectar
    cuando dos títulos parecidos en texto en realidad hablan de umbrales
    distintos (ej. "$100k" vs "$150k")."""
    result = set()
    for raw, suffix in _NUMBER_RE.findall(text):
        cleaned = raw.replace(",", "")
        if not cleaned or cleaned == ".":
            continue
        try:
            value = float(cleaned)
        except ValueError:
            continue
        # Un número de 4 dígitos sin sufijo/decimal/coma en rango de año
        # calendario (1900-2099) es casi seguro un AÑO mencionado en el
        # título (ej. "...by December 2026"), no un umbral/monto del
        # mercado -- si se lo cuenta como número, dos mercados de años
        # distintos pero con el mismo umbral podrían dar "no coincide"
        # por el año en vez del umbral real, o al revés, dos mercados de
        # temas totalmente distintos podrían "coincidir" solo porque
        # mencionan el mismo año.
        if not suffix and 1900 <= value <= 2099 and "." not in raw and "," not in raw:
            continue
        multiplier = _NUMBER_MULTIPLIERS.get((suffix or "").lower(), 1)
        result.add(round(value * multiplier, 4))
    return result


def _numbers_align(poly_title: str, kalshi_title: str) -> bool | None:
    """True si algún monto/umbral mencionado en un título coincide con
    alguno del otro. False si ambos títulos mencionan números pero
    ninguno coincide (señal fuerte de mercado distinto, ej. $100k vs
    $150k). None si alguno de los dos (o ambos) no menciona ningún
    número -- sin números de un lado no hay nada que comparar, no
    penaliza."""
    a, b = _extract_numbers(poly_title), _extract_numbers(kalshi_title)
    if not a or not b:
        return None
    return not a.isdisjoint(b)


# Pares de dirección semántica opuesta -- si un título usa una palabra de
# un lado y el otro título usa una del lado opuesto, probablemente son
# mercados DISTINTOS (ej. "Fed increases rates" vs "Fed decreases rates"),
# no el mismo evento con un Yes/No invertido. Lista de mejor esfuerzo, no
# exhaustiva -- cubre los casos más comunes en títulos de mercados de
# predicción (economía, deportes, elecciones).
_DIRECTION_UP = {
    "increase", "increases", "increasing", "rise", "rises", "rising",
    "up", "gain", "gains", "win", "wins", "winning", "higher", "above",
    "exceed", "exceeds", "more", "over", "hike", "hikes",
}
_DIRECTION_DOWN = {
    "decrease", "decreases", "decreasing", "fall", "falls", "falling",
    "down", "drop", "drops", "lose", "loses", "losing", "lower", "below",
    "under", "less", "cut", "cuts",
}


def _direction_tokens(text: str) -> set[str]:
    words = set(re.findall(r"[a-z]+", text.lower()))
    tokens = set()
    if words & _DIRECTION_UP:
        tokens.add("up")
    if words & _DIRECTION_DOWN:
        tokens.add("down")
    return tokens


def _direction_conflict(poly_title: str, kalshi_title: str) -> bool:
    """True si un título usa lenguaje de dirección "sube" y el otro
    "baja" (y ninguno de los dos usa ambos, lo que sería ambiguo) --
    señal fuerte de que son mercados distintos, no un Yes/No invertido
    del mismo evento."""
    a, b = _direction_tokens(poly_title), _direction_tokens(kalshi_title)
    if a == {"up"} and b == {"down"}:
        return True
    if a == {"down"} and b == {"up"}:
        return True
    return False


# Alternativas ordenadas por longitud descendente -- mismo motivo que
# _NUMBER_RE: "sep" antes que "sept" cortaría "sept" a la mitad.
_MONTH_NAMES = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sept": 9, "sep": 9, "october": 10, "oct": 10,
    "november": 11, "nov": 11, "december": 12, "dec": 12,
}
_MONTH_YEAR_RE = re.compile(
    r"\b(" + "|".join(sorted(_MONTH_NAMES, key=len, reverse=True)) + r")\.?\s+"
    r"(?:\d{1,2}(?:st|nd|rd|th)?,?\s+)?(\d{4})\b",
    re.IGNORECASE,
)


def _extract_month_years(text: str) -> set[tuple[int, int]]:
    """Extrae pares (mes, año) mencionados en el título (ej. "September
    2026", "Dec 31, 2026" -> {(9, 2026)}, {(12, 2026)}) -- un mercado con
    fecha de expiración/resolución puntual (ej. "¿pasa esto antes de
    marzo 2027?") NO es el mismo mercado que uno sin esa fecha o con una
    distinta, aunque el resto de la pregunta sea idéntico."""
    result = set()
    for month_str, year_str in _MONTH_YEAR_RE.findall(text):
        month = _MONTH_NAMES.get(month_str.lower())
        if month:
            result.add((month, int(year_str)))
    return result


def _month_year_align(poly_title: str, kalshi_title: str) -> bool | None:
    """True si algún (mes, año) mencionado en un título coincide con
    alguno del otro. False en dos casos, ambos señal de mercados
    distintos: (a) los dos mencionan mes+año pero ninguno coincide, o
    (b) SOLO uno de los dos títulos menciona mes+año -- si un mercado
    tiene una fecha de corte explícita y el otro no dice nada al
    respecto, no hay forma de asumir que resuelven en el mismo momento.
    None solo cuando NINGUNO de los dos títulos menciona mes+año (sin
    señal de fecha en ningún lado, no se puede comparar, no penaliza)."""
    a, b = _extract_month_years(poly_title), _extract_month_years(kalshi_title)
    if not a and not b:
        return None
    if bool(a) != bool(b):
        return False
    return not a.isdisjoint(b)


def generate_candidates(
    poly_markets: list[dict], kalshi_markets: list[dict], min_score: float,
    max_price_spread: float | None = None,
) -> list[dict]:
    candidates = []
    for pm in poly_markets:
        poly_price = _implied_yes_price_polymarket(pm)
        for km in kalshi_markets:
            score = fuzz.token_sort_ratio(pm["title"], km["title"])
            if score < min_score:
                continue

            kalshi_price = _implied_yes_price_kalshi(km)
            # El "Yes" de una plataforma no siempre corresponde al "Yes"
            # de la otra -- confirmado a mano con el par de la Fed en
            # config/market_mapping.json (dos títulos casi idénticos,
            # pero el "Yes" de Polymarket resultó equivalente al "No" de
            # Kalshi, no a su "Yes"). Un matcher que solo compara Yes
            # contra Yes descarta ese tipo de par real por "spread
            # enorme" cuando en realidad está comparando lados opuestos.
            # Se prueban las dos orientaciones y se usa la que dé spread
            # más chico -- eso es lo que de verdad indica si son
            # equivalentes, no la orientación literal del label "Yes".
            if poly_price is not None and kalshi_price is not None:
                spread_direct = abs(poly_price - kalshi_price)
                spread_inverted = abs(poly_price - (1 - kalshi_price))
                inverted = spread_inverted < spread_direct
                price_spread = round(spread_inverted if inverted else spread_direct, 4)
            else:
                inverted = False
                price_spread = None
            # Solo se descarta cuando SÍ se pudo calcular el spread y se
            # pasó el evento -- si falta el precio de algún lado (mercado
            # recién abierto sin trades, por ejemplo) no se penaliza al
            # candidato por eso, se lo deja pasar sin ese chequeo.
            if max_price_spread is not None and price_spread is not None and price_spread > max_price_spread:
                continue

            # Arbitraje real vs. simple "spread bajo": el margen teórico de
            # arbitraje termina siendo, matemáticamente, el mismo número que
            # price_spread (comprar el lado barato en cada plataforma
            # siempre cuesta 1 - price_spread combinado) -- lo que agrega
            # valor acá no es un número nuevo, es traducir eso en una
            # instrucción concreta de qué comprar en cada plataforma, según
            # si están alineadas (mismo lado) o invertidas (lados opuestos
            # del mismo evento). Se ignoran spreads chicos (por debajo de
            # ARBITRAGE_MIN_MARGIN) porque ahí las fees/slippage reales
            # probablemente se comen el margen. Esto asume que de verdad es
            # el MISMO evento en ambas plataformas -- si semantic_flags de
            # abajo tiene alguna advertencia (no solo "Coincide"), puede que
            # sea la fila "simplemente relacionados" de la tabla, no
            # arbitraje real, y el dashboard lo marca así.
            arbitrage_margin = None
            arbitrage_detail = None
            if poly_price is not None and kalshi_price is not None and price_spread is not None:
                if price_spread >= ARBITRAGE_MIN_MARGIN:
                    arbitrage_margin = price_spread
                    if inverted:
                        # "Yes" de una plataforma equivale al "No" de la
                        # otra (mismo evento, lados opuestos) -- comprar el
                        # mismo lado real en ambas (los dos "Yes" o los dos
                        # "No", el que sume menos de $1) cubre el evento.
                        if poly_price + kalshi_price < 1:
                            leg_a, leg_b = "'Yes' en Polymarket", "'Yes' en Kalshi"
                            cost = poly_price + kalshi_price
                        else:
                            leg_a, leg_b = "'No' en Polymarket", "'No' en Kalshi"
                            cost = (1 - poly_price) + (1 - kalshi_price)
                    else:
                        # "Yes" de una plataforma equivale al "Yes" de la
                        # otra (mismo evento, mismo lado) -- comprar el lado
                        # que dice "Yes" en la plataforma más barata y "No"
                        # en la otra cubre el evento.
                        if poly_price < kalshi_price:
                            leg_a, leg_b = "'Yes' en Polymarket", "'No' en Kalshi"
                            cost = poly_price + (1 - kalshi_price)
                        else:
                            leg_a, leg_b = "'No' en Polymarket", "'Yes' en Kalshi"
                            cost = (1 - poly_price) + kalshi_price
                    arbitrage_detail = (
                        f"Comprar {leg_a} y {leg_b} cuesta ${cost:.2f} combinado -- si de "
                        f"verdad es el mismo evento, paga $1 seguro (margen "
                        f"${arbitrage_margin:.2f}, antes de fees/slippage)."
                    )

            # Score combinado (0-100) -- fórmula, no "razonamiento" real
            # sobre el significado de la pregunta (para eso haría falta un
            # LLM, que el usuario decidió no usar acá): pondera similitud
            # de texto Y cercanía de precio en un solo número, en vez de
            # ordenar por separado. price_score = 100 cuando el spread es
            # 0, 0 cuando el spread es 1 (extremos opuestos) -- misma
            # escala 0-1 que price_spread, así que es una resta directa.
            #
            # Por qué NO pesar el precio mucho más que el texto (tentador,
            # ya que se documentó que el precio es la señal más confiable
            # para descartar falsos positivos con título parecido): dos
            # mercados SIN relación entre sí pueden coincidir en precio
            # por pura casualidad si ambos son eventos "casi seguros" o
            # "casi imposibles" (ej. dos mercados al 99% de cosas
            # totalmente distintas) -- el texto sigue haciendo falta para
            # no confundir esa coincidencia con equivalencia real. 45/55
            # es un balance, no dejar que ninguna señal sola decida.
            price_score = round((1 - price_spread) * 100, 1) if price_spread is not None else None
            combined_score = (
                round(0.45 * score + 0.55 * price_score, 1)
                if price_score is not None
                # Sin precio de algún lado no hay forma de aplicar la mitad
                # de la fórmula -- se usa el score de texto solo, pero se
                # lo marca (price_score None) para que no compita en pie de
                # igualdad con uno sí verificado por precio (ver sort abajo).
                else round(score, 1)
            )

            # Relacionar las preguntas más allá de similitud de caracteres
            # cruda: un título "sube" contra uno "baja" es casi siempre un
            # mercado DISTINTO, no un Yes/No invertido -- se descuenta
            # fuerte. Un umbral/monto que no coincide entre títulos
            # parecidos (ej. $100k vs $150k) también es señal fuerte de
            # mercado distinto -- mismo tratamiento. Un umbral que SÍ
            # coincide suma confianza. Todo esto queda visible en
            # semantic_flags, nunca oculto -- el usuario decide con esa
            # info, la fórmula no descarta el candidato por su cuenta.
            semantic_flags = []
            direction_conflict = _direction_conflict(pm["title"], km["title"])
            if direction_conflict:
                combined_score = round(combined_score * 0.3, 1)
                semantic_flags.append(
                    "Direcciones opuestas en el título (ej. sube vs. baja, gana vs. pierde) -- "
                    "probablemente NO es el mismo mercado, no solo un Yes/No invertido."
                )

            numbers_match = _numbers_align(pm["title"], km["title"])
            if numbers_match is False:
                combined_score = round(combined_score * 0.4, 1)
                semantic_flags.append(
                    "Los montos/umbrales mencionados en cada título no coinciden -- "
                    "probablemente es un mercado con un umbral distinto (ej. $100k vs $150k)."
                )
            elif numbers_match is True:
                combined_score = round(min(100, combined_score + 5), 1)
                semantic_flags.append("Coincide un monto/umbral numérico entre ambos títulos.")

            month_year_match = _month_year_align(pm["title"], km["title"])
            if month_year_match is False:
                combined_score = round(combined_score * 0.5, 1)
                semantic_flags.append(
                    "Los meses/años mencionados en los títulos no coinciden (o solo uno de los "
                    "dos títulos menciona una fecha puntual) -- probablemente resuelven en "
                    "momentos distintos."
                )
            elif month_year_match is True:
                combined_score = round(min(100, combined_score + 5), 1)
                semantic_flags.append("Coincide el mes/año mencionado en ambos títulos.")

            candidates.append({
                "score": round(score, 1),
                "price_score": price_score,
                "combined_score": combined_score,
                "semantic_flags": semantic_flags,
                "polymarket_title": pm["title"],
                "polymarket_condition_id": pm["condition_id"],
                "polymarket_token_ids": pm["token_ids"],
                "polymarket_outcomes": pm.get("outcomes"),
                "polymarket_volume_24h": pm.get("volume_24h"),
                "polymarket_yes_price": poly_price,
                "kalshi_title": km["title"],
                "kalshi_ticker": km["ticker"],
                "kalshi_event_ticker": km["event_ticker"],
                "kalshi_volume_24h": km.get("volume_24h"),
                "kalshi_yes_price": kalshi_price,
                "price_spread": price_spread,
                # True => el "Yes" de Polymarket corresponde al "No" de
                # Kalshi (y viceversa) -- ver POST /discover/confirm_mirror
                # en api/main.py, que usa esto para armar bien el mapeo de
                # outcome_id en vez de asumir siempre Yes<->Yes.
                "kalshi_inverted": inverted,
                # None => no se pudo calcular (falta precio de algún
                # lado) o no hay arbitraje con los precios actuales.
                # Número => margen garantizado por cada $1 apostado si se
                # ejecutan las dos patas ahora mismo (antes de fees/slippage).
                "arbitrage_margin": arbitrage_margin,
                "arbitrage_detail": arbitrage_detail,
            })

    # Orden: primero los que tienen arbitraje real detectado (margen
    # garantizado, no solo spread bajo) ordenados por margen; después el
    # resto de los que sí tienen precio de ambos lados (más confiables, la
    # fórmula pudo aplicarse completa) por combined_score descendente. Un
    # candidato sin datos de precio (price_score None) no es descartado,
    # pero tampoco compite de igual a igual contra uno sí verificado por
    # precio.
    candidates.sort(key=lambda c: (
        0 if c["arbitrage_margin"] is not None else 1,
        -(c["arbitrage_margin"] or 0),
        0 if c["price_score"] is not None else 1,
        -c["combined_score"],
    ))
    return candidates


def find_mirror_candidates(
    limit_per_platform: int = 200,
    min_score: float = 45.0,
    min_volume: float | None = None,
    max_kalshi_pages: int = 8,
    top_n: int = 25,
    max_price_spread: float | None = DEFAULT_MAX_PRICE_SPREAD,
    keywords: list[str] | None = None,
) -> list[dict]:
    """Punto de entrada para el panel "Sugerencias de mercados espejo" del
    dashboard (api/main.py: GET /discover/mirror_candidates).

    Sin `keywords`: compara "lo más movido en volumen" de cada plataforma
    (fetch_trending_*) -- rápido, pero en la práctica Polymarket y Kalshi
    no siempre tienen los mismos temas arriba de la tabla de volumen, así
    que puede no encontrar nada aunque el mercado exista en las dos.

    Con `keywords`: recorre el catálogo activo completo filtrado por esas
    palabras -- más lento, pero mucho más efectivo para un tema puntual
    (confirmado en vivo con "bitcoin": el listado general de Kalshi está
    dominado por miles de props deportivos que sepultan un tema de nicho
    mucho antes de una paginación razonable). Para esto se combinan dos
    rutas por plataforma: la ruta rápida por categoría/tag
    (fetch_kalshi_markets_via_series / fetch_polymarket_markets_via_tags)
    y la paginación genérica por palabra clave (fetch_kalshi_markets /
    fetch_polymarket_markets, acotada a DISCOVER_MAX_PAGES_MIRROR
    páginas) -- se deduplica por condition_id/ticker, se prioriza lo que
    trajo la ruta rápida."""
    if keywords:
        poly_via_tags = fetch_polymarket_markets_via_tags(keywords, min_volume)
        poly_via_pages = fetch_polymarket_markets(keywords, 500, DISCOVER_MAX_PAGES_MIRROR, min_volume)
        seen_condition_ids = {m["condition_id"] for m in poly_via_tags if m.get("condition_id")}
        poly_markets = poly_via_tags + [
            m for m in poly_via_pages if m.get("condition_id") not in seen_condition_ids
        ]
        kalshi_via_series = fetch_kalshi_markets_via_series(keywords, min_volume)
        kalshi_via_pages = fetch_kalshi_markets(keywords, 200, DISCOVER_MAX_PAGES_MIRROR, min_volume)
        seen_kalshi_tickers = {m["ticker"] for m in kalshi_via_series if m.get("ticker")}
        kalshi_markets = kalshi_via_series + [
            m for m in kalshi_via_pages if m.get("ticker") not in seen_kalshi_tickers
        ]
    else:
        poly_markets = fetch_trending_polymarket(limit=limit_per_platform, min_volume=min_volume)
        kalshi_markets = fetch_trending_kalshi(
            limit=limit_per_platform, max_pages=max_kalshi_pages, min_volume=min_volume,
        )

    if not poly_markets or not kalshi_markets:
        return []

    candidates = generate_candidates(poly_markets, kalshi_markets, min_score, max_price_spread)
    return candidates[:top_n]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sugiere pares candidatos de mercados equivalentes entre Polymarket y Kalshi (paso 6)."
    )
    parser.add_argument(
        "--keywords", nargs="+", required=True,
        help="Palabras clave para filtrar mercados por título antes de comparar (ej: fed election recession)",
    )
    parser.add_argument(
        "--min-score", type=float, default=55.0,
        help="Umbral mínimo de similitud (0-100) para que un par se guarde como candidato. Default: 55",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"Buscando mercados de Polymarket con keywords: {args.keywords}")
    poly_markets = fetch_polymarket_markets(args.keywords)
    print(f"  -> {len(poly_markets)} mercados encontrados")

    print(f"Buscando mercados de Kalshi con keywords: {args.keywords}")
    kalshi_markets = fetch_kalshi_markets(args.keywords)
    print(f"  -> {len(kalshi_markets)} mercados encontrados")

    if not poly_markets or not kalshi_markets:
        print("No hay suficientes mercados de ambos lados para comparar. Probá con otras keywords.")
        sys.exit(0)

    candidates = generate_candidates(poly_markets, kalshi_markets, args.min_score)

    if not candidates:
        print(f"No se encontraron pares con score >= {args.min_score}. Probá bajar --min-score.")
        sys.exit(0)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"market_pair_candidates_{today}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)

    print(f"\n{len(candidates)} pares candidatos guardados en: {out_path}")
    print("\nTop 10 por score (revisar a mano antes de confirmar en config/market_mapping.json):\n")
    for c in candidates[:10]:
        print(f"  [{c['score']}] {c['polymarket_title']!r}  <->  {c['kalshi_title']!r}")
        print(f"        polymarket: {c['polymarket_condition_id']}")
        print(f"        kalshi:     {c['kalshi_ticker']}")
        print()

    print("Recordatorio: un score alto NO garantiza que sea el mismo evento (ni que las reglas de")
    print("resolución coincidan). Confirmá cada par a mano antes de agregarlo a market_mapping.json.")


if __name__ == "__main__":
    main()
