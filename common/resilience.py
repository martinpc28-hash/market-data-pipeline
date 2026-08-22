"""
Fase 3 — Tolerancia a fallos: backoff exponencial para reconexión, y
detección de gaps con dos estrategias distintas según el feed (documentado
en polymarket-kalshi-websocket-research.md):

- Kalshi: determinística, por número de secuencia (`seq`). Un salto en la
  secuencia es un gap real, sin ambigüedad.
- Polymarket: heurística, por silencio. No hay `seq`, así que solo podemos
  inferir un gap si pasó "demasiado" tiempo sin mensajes. Esto puede dar
  falsos positivos (el mercado genuinamente no tuvo actividad) o falsos
  negativos (perdimos un mensaje pero llegó el siguiente antes del timeout).
  Es una limitación real de la API, no del código -- hay que ajustar el
  timeout según qué tan activo se espera que esté el mercado.

Todo gap detectado se loggea en dos formas: una línea humana en
logs/gaps.log (vía common.logging_utils.gap_logger) y un registro
estructurado en logs/gaps.jsonl (para poder analizarlos programáticamente
más adelante, por ejemplo para el reporte de "Stress testing" del README).
"""

import json
import random
from datetime import datetime, timezone
from pathlib import Path

GAPS_JSONL_PATH = Path(__file__).resolve().parent.parent / "logs" / "gaps.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def log_gap(gap_logger, source: str, gap_type: str, start_ts: str, end_ts: str, detail: str = ""):
    """Punto único por el que pasa TODO gap detectado, sea del tipo que sea.
    Cero pérdida de datos silenciosa: si esto se llama, algo se documenta,
    nunca se descarta calladamente."""
    try:
        duration = (_parse_iso(end_ts) - _parse_iso(start_ts)).total_seconds()
    except Exception:
        duration = None

    dur_str = f"{duration:.2f}s" if duration is not None else "desconocida"
    gap_logger.warning(
        "[%s] GAP tipo=%s inicio=%s fin=%s duración=%s detalle=%s",
        source, gap_type, start_ts, end_ts, dur_str, detail,
    )

    GAPS_JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "logged_at": _now_iso(),
        "source": source,
        "gap_type": gap_type,
        "start": start_ts,
        "end": end_ts,
        "duration_seconds": duration,
        "detail": detail,
    }
    with GAPS_JSONL_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()


class ExponentialBackoff:
    """Backoff exponencial con jitter para reconexión (paso 7).
    Uso: delay = backoff.next_delay(); ...; backoff.reset() cuando la
    conexión se mantiene estable por encima de STABLE_THRESHOLD segundos."""

    def __init__(self, base: float = 1.0, cap: float = 60.0, multiplier: float = 2.0, jitter: float = 0.3):
        self.base = base
        self.cap = cap
        self.multiplier = multiplier
        self.jitter = jitter
        self.attempt = 0

    def next_delay(self) -> float:
        raw = min(self.cap, self.base * (self.multiplier ** self.attempt))
        self.attempt += 1
        jitter_amount = raw * self.jitter
        return max(0.0, raw + random.uniform(-jitter_amount, jitter_amount))

    def reset(self):
        self.attempt = 0


class KalshiGapTracker:
    """Gap detection determinística vía `seq`. Importante (lo descubrimos
    probando en vivo): `seq` es por SESIÓN de conexión, no global -- cada
    `orderbook_snapshot` nuevo arranca de nuevo. Por eso on_snapshot no
    compara nada, solo resetea el contador esperado para ese mercado."""

    def __init__(self, gap_logger):
        self.gap_logger = gap_logger
        self._last = {}  # outcome_id -> (seq, received_at_iso)

    def on_snapshot(self, outcome_id: str, seq: int | None, received_at: str):
        if outcome_id and seq is not None:
            self._last[outcome_id] = (seq, received_at)

    def on_delta(self, outcome_id: str, seq: int | None, received_at: str):
        if not outcome_id or seq is None:
            return

        prev = self._last.get(outcome_id)
        if prev is not None:
            prev_seq, prev_ts = prev
            if seq != prev_seq + 1:
                log_gap(
                    self.gap_logger, "kalshi", "sequence_gap",
                    start_ts=prev_ts, end_ts=received_at,
                    detail=f"outcome={outcome_id} seq saltó de {prev_seq} a {seq} (faltan {seq - prev_seq - 1} mensaje(s))",
                )
        self._last[outcome_id] = (seq, received_at)


class SilenceGapTracker:
    """Gap detection heurística por timeout, para feeds sin número de
    secuencia (Polymarket). Se alimenta con on_message() cada vez que llega
    un mensaje de datos real, y check_timeout() se llama periódicamente
    desde una tarea de fondo (ver watchdog más abajo)."""

    def __init__(self, source: str, timeout_seconds: float, gap_logger):
        self.source = source
        self.timeout_seconds = timeout_seconds
        self.gap_logger = gap_logger
        self.last_received: datetime | None = None
        self.in_gap = False
        self.gap_start: datetime | None = None

    def on_message(self, received_at: datetime):
        if self.in_gap:
            log_gap(
                self.gap_logger, self.source, "silence_gap",
                start_ts=self.gap_start.isoformat(), end_ts=received_at.isoformat(),
                detail=f"sin mensajes por más de {self.timeout_seconds}s",
            )
            self.in_gap = False
            self.gap_start = None
        self.last_received = received_at

    def check_timeout(self, now: datetime):
        if self.last_received is None or self.in_gap:
            return
        elapsed = (now - self.last_received).total_seconds()
        if elapsed > self.timeout_seconds:
            self.in_gap = True
            self.gap_start = self.last_received


async def silence_watchdog(tracker: SilenceGapTracker, interval_seconds: float = 5.0):
    """Tarea de fondo que llama check_timeout() cada `interval_seconds`.
    Se cancela cuando el feed se desconecta (ver finally en el loop principal)."""
    import asyncio
    try:
        while True:
            await asyncio.sleep(interval_seconds)
            tracker.check_timeout(datetime.now(timezone.utc))
    except asyncio.CancelledError:
        pass
