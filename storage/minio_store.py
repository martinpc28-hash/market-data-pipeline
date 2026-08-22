"""
Fase 4 — Archivado de los JSONL crudos en MinIO (object storage).

TimescaleDB guarda los eventos ya normalizados, listos para consultar
(Fase 5). MinIO guarda el crudo exacto que llegó de cada exchange, sin
tocar -- es la copia de "verdad última" para poder reproducir la ingesta o
auditar un bug de normalización, y además sirve como backup fuera del
disco local donde corre el proceso de ingesta.

La subida es periódica (ver ARCHIVE_INTERVAL_SECONDS en
ingestion/run_resilient_feed.py) y sobre-escribe el mismo objeto cada vez
-- no versiona por snapshot, porque los JSONL son append-only localmente,
así que cada subida es un superset de la anterior. Es una decisión
deliberada para mantenerlo simple en esta fase; versionar por snapshot con
bucket versioning de MinIO queda como mejora posible, no es necesario para
el objetivo del proyecto.

Importante: un fallo subiendo a MinIO NUNCA debe tirar abajo la ingesta.
El archivo local ya está a salvo (se escribió antes, en Fase 1/2) con o sin
MinIO -- si MinIO está caído, se loggea y se reintenta en el próximo ciclo,
nada más.
"""

import os
from pathlib import Path

from minio import Minio


def client_from_env() -> Minio:
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def bucket_from_env() -> str:
    return os.environ.get("MINIO_BUCKET", "market-data-raw")


def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def archive_directory(
    client: Minio,
    bucket: str,
    local_dir: Path,
    prefix: str,
    logger,
    patterns=("*.jsonl",),
) -> int:
    """Sube todos los archivos que matcheen `patterns` dentro de local_dir,
    preservando el nombre de archivo bajo `prefix/`. Devuelve cuántos se
    subieron con éxito. No lanza excepción si una subida individual falla
    (se loggea y se sigue con el resto)."""
    if not local_dir.exists():
        return 0

    uploaded = 0
    for pattern in patterns:
        for path in sorted(local_dir.glob(pattern)):
            object_name = f"{prefix}/{path.name}"
            try:
                client.fput_object(bucket, object_name, str(path))
                uploaded += 1
            except Exception:
                logger.exception("[minio] Falló subiendo %s a %s/%s", path, bucket, object_name)
    return uploaded
