"""
Fase 7 (panel de Cuentas/Portfolio) — Cliente REST autenticado para la
API de portfolio de Kalshi (balance, posiciones, fills, settlements).

Reutiliza las MISMAS credenciales que ya usa la ingesta por WebSocket
(ingestion/kalshi_ingest.py: KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH
en .env) -- es la misma cuenta, mismo mecanismo de firma RSA-PSS
(confirmado contra la documentación real de Kalshi: el REST firma
`timestamp_ms + METODO + path` -- igual que el WS, solo que el "path" acá
es la ruta REST sin query string, con el prefijo /trade-api/v2 incluido).

Import intencionalmente independiente de ingestion/kalshi_ingest.py (que
vive en el paquete de ingesta, no se copia a la imagen de la API) para no
crear un acoplamiento cruzado entre servicios -- common/ ya se comparte
entre api e ingestion, así que este módulo vive acá.

Este módulo SOLO hace requests de lectura (GET) -- no hay ninguna función
para crear/cancelar órdenes. El objetivo es mostrar el portfolio en el
dashboard, no operar desde acá.
"""

import base64
import os
import time
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
API_PREFIX = "/trade-api/v2"

# Confirmado contra docs.kalshi.com: la URL base REST de prod es
# api.elections.kalshi.com (la misma que ya usa tools/market_matcher.py
# para datos públicos de mercado); demo tiene su propio host.
REST_BASES = {
    "demo": "https://external-api.demo.kalshi.co",
    "prod": "https://api.elections.kalshi.com",
}


class KalshiAuthError(Exception):
    """Credenciales de Kalshi faltantes o inválidas -- separado de errores
    de red para que el endpoint pueda devolver un mensaje claro ("no hay
    cuenta conectada") en vez de un 502 genérico."""


def _load_credentials():
    load_dotenv(PROJECT_ROOT / ".env")

    api_key_id = os.environ.get("KALSHI_API_KEY_ID")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    env_name = os.environ.get("KALSHI_ENV", "demo").strip().lower()

    if not api_key_id or api_key_id == "tu_key_id_aqui":
        raise KalshiAuthError("Falta KALSHI_API_KEY_ID en .env (o quedó con el valor de ejemplo).")
    if not key_path:
        raise KalshiAuthError("Falta KALSHI_PRIVATE_KEY_PATH en .env.")

    full_key_path = PROJECT_ROOT / key_path
    if not full_key_path.exists():
        raise KalshiAuthError(f"No se encontró la clave privada en: {full_key_path}")

    with full_key_path.open("rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    if env_name not in REST_BASES:
        raise KalshiAuthError(f"KALSHI_ENV inválido: {env_name!r} (usar 'demo' o 'prod')")

    return api_key_id, private_key, env_name


def _sign(api_key_id: str, private_key, method: str, path: str) -> dict:
    """Idéntico al de ingestion/kalshi_ingest.py::build_auth_headers, salvo
    que acá `path` es una ruta REST (ej: /trade-api/v2/portfolio/balance)
    en vez de la ruta del WebSocket -- el esquema de firma es el mismo."""
    timestamp_ms = str(int(time.time() * 1000))
    message = f"{timestamp_ms}{method}{path}".encode("utf-8")

    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256().digest_size),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }


def kalshi_rest_get(path: str, params: dict | None = None, timeout: float = 15.0) -> dict:
    """GET autenticado contra la API de portfolio de Kalshi. `path` sin el
    prefijo /trade-api/v2 (se agrega acá) y sin query string (va en
    `params`, la firma es solo sobre el path -- ver docstring de _sign)."""
    api_key_id, private_key, env_name = _load_credentials()
    full_path = API_PREFIX + path
    url = REST_BASES[env_name] + full_path
    headers = _sign(api_key_id, private_key, "GET", full_path)

    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def get_balance() -> dict:
    return kalshi_rest_get("/portfolio/balance")


def get_positions(limit: int = 200, max_pages: int = 3) -> list[dict]:
    """Posiciones abiertas (count_filter=position -- descarta las que ya
    están en cero). Paginado y acotado igual que el resto del proyecto
    (ver DISCOVER_MAX_PAGES en api/main.py): no hay razón para traer más
    de unos pocos cientos de posiciones para un panel de resumen."""
    positions = []
    cursor = None
    pages = 0
    while pages < max_pages:
        params = {"limit": limit, "count_filter": "position"}
        if cursor:
            params["cursor"] = cursor
        body = kalshi_rest_get("/portfolio/positions", params=params)
        page = body.get("market_positions", body.get("positions", []))
        positions.extend(page)
        cursor = body.get("cursor")
        pages += 1
        if not cursor or not page:
            break
    return positions


def get_fills(limit: int = 100, max_pages: int = 5) -> list[dict]:
    """Historial de ejecuciones (fills) -- para la tabla de "trades
    históricos" del panel. Más reciente primero (así viene de la API)."""
    fills = []
    cursor = None
    pages = 0
    while pages < max_pages:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        body = kalshi_rest_get("/portfolio/fills", params=params)
        page = body.get("fills", [])
        fills.extend(page)
        cursor = body.get("cursor")
        pages += 1
        if not cursor or not page:
            break
    return fills


def get_settlements(limit: int = 200, max_pages: int = 10) -> list[dict]:
    """Mercados ya liquidados -- es la fuente más confiable para el P&L
    realizado (revenue - costo - fees, todo lo da la propia liquidación
    de Kalshi, no hace falta reconstruir contabilidad de fills a mano).
    Usado tanto para las métricas del panel como para la curva de
    evolución de P&L acumulado."""
    settlements = []
    cursor = None
    pages = 0
    while pages < max_pages:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        body = kalshi_rest_get("/portfolio/settlements", params=params)
        page = body.get("settlements", [])
        settlements.extend(page)
        cursor = body.get("cursor")
        pages += 1
        if not cursor or not page:
            break
    return settlements


def settlement_pnl(settlement: dict) -> float:
    """P&L realizado de UNA liquidación, en dólares: lo cobrado (`revenue`,
    en centavos) menos el costo de los contratos que terminaron pagando
    (yes_total_cost_dollars o no_total_cost_dollars según el resultado)
    menos las fees. `market_result` puede ser 'yes'/'no'/'scalar' -- para
    'scalar' no hay un costo binario claro, así que se usa el costo total
    (yes+no) como aproximación y se marca en el campo devuelto."""
    revenue = float(settlement.get("revenue") or 0) / 100.0
    fee = float(settlement.get("fee_cost") or 0)
    result = settlement.get("market_result")

    yes_cost = float(settlement.get("yes_total_cost_dollars") or 0)
    no_cost = float(settlement.get("no_total_cost_dollars") or 0)
    if result == "yes":
        cost = yes_cost
    elif result == "no":
        cost = no_cost
    else:
        cost = yes_cost + no_cost

    return round(revenue - cost - fee, 4)
