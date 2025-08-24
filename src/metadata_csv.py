# Recorre la carpeta local "raw/" y guarda metadatos en un CSV local.
from datetime import datetime, timezone
from typing import Optional
import hashlib
from pathlib import Path

import pandas as pd
from PIL import Image

RAW_ROOT = Path("raw")
OUTPUT_DIR = Path("outputs")
OUTPUT_CSV = OUTPUT_DIR / "images_metadata.csv"
BATCH_SIZE = 500

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}

def parse_local_parts(rel_dir_parts: tuple[str, ...]):
    """
    Estructura esperada: raw/<dataset>/<scenario>/<split>/.../<filename>
    rel_dir_parts son los directorios desde 'raw/' sin el filename.
    """
    return {
        "origin":   "raw",
        "dataset":  rel_dir_parts[0] if len(rel_dir_parts) > 0 else None,
        "scenario": rel_dir_parts[1] if len(rel_dir_parts) > 1 else None,
        "split":    rel_dir_parts[2] if len(rel_dir_parts) > 2 else None,
    }

def derive_zip_name_from_dataset(dataset: Optional[str]) -> Optional[str]:
    # Mantiene la convención de nombres por si se quiere conservar en el CSV
    if not dataset:
        return None
    return f"{dataset}_images.zip"

def image_size_from_path(p: Path):
    '''
    Devuelve (width, height) si puede; si no, (None, None).
    '''
    try:
        with Image.open(p) as img:
            return img.width, img.height
    except Exception:
        return None, None

def compute_sha256_file(p: Path) -> Optional[str]:
    '''
    Calcula el hash SHA-256 de un archivo.
    '''
    h = hashlib.sha256()
    try:
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def write_batch_csv(rows, header_written):
    df = pd.DataFrame(rows)
    if "ts_ingest" in df.columns:
        df["ts_ingest"] = pd.to_datetime(df["ts_ingest"], utc=True)
    for c in ["width", "height"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df.to_csv(OUTPUT_CSV, mode="a", index=False, header=not header_written)
    return header_written or len(df) > 0

def run():
    if not RAW_ROOT.is_dir():
        raise SystemExit(f"No existe la carpeta local: {RAW_ROOT.resolve()}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    total = 0
    header_written = OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size > 0

    for p in RAW_ROOT.rglob("*"):
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext not in IMAGE_EXTS:
            continue

        try:
            rel = p.relative_to(RAW_ROOT)
        except ValueError:
            continue

        rel_dir_parts = rel.parent.parts  # Sin el filename.
        meta = parse_local_parts(rel_dir_parts)

        width, height = image_size_from_path(p)
        sha256 = compute_sha256_file(p)
        zip_name = derive_zip_name_from_dataset(meta["dataset"])

        # Usamos mtime como "ts_ingest" local (UTC)
        try:
            ts_ingest_dt = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        except Exception:
            ts_ingest_dt = datetime.now(timezone.utc)

        rows.append({
            # Mantengo el nombre 'gcs_uri' por compatibilidad con el esquema.
            "gcs_uri":   f"file://{p.resolve()}",
            "origin":    meta["origin"],
            "dataset":   meta["dataset"],
            "scenario":  meta["scenario"],
            "split":     meta["split"],
            "filename":  p.name,
            "ext":       ext.lstrip("."),
            "width":     width,
            "height":    height,
            "sha256":    sha256,
            "zip_name":  zip_name,
            "ts_ingest": ts_ingest_dt.isoformat(),
        })

        if len(rows) >= BATCH_SIZE:
            header_written = write_batch_csv(rows, header_written)
            total += len(rows)
            rows.clear()
            print(f"Escritas {total} filas en {OUTPUT_CSV}...")

    if rows:
        header_written = write_batch_csv(rows, header_written)
        total += len(rows)

    print(f"Completado. Filas totales escritas: {total}")
    print(f"CSV: {OUTPUT_CSV.resolve()}")
