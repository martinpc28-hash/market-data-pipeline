"""Logging compartido entre los distintos scripts de ingesta, para no
duplicar la misma configuración en cada uno (antes vivía repetida en
polymarket_ingest.py y kalshi_ingest.py)."""

import logging
import sys
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"


def setup_logging(name: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        # Evita duplicar handlers si setup_logging se llama más de una vez
        # con el mismo nombre (pasa en tests o al reimportar en un notebook).
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_DIR / f"{name}.log")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


def gap_logger() -> logging.Logger:
    """Logger dedicado para gaps detectados (Fase 3, paso 9 del roadmap).
    Separado del logger general de cada feed para que sea fácil de ubicar
    y de vigilar ("log dedicado y visible")."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("gaps")
    logger.setLevel(logging.WARNING)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_DIR / "gaps.log")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger
