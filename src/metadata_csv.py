from datetime import datetime, timezone
from typing import Optional, List, Tuple
import hashlib
from pathlib import Path

import pandas as pd
from PIL import Image

RAW_ROOT = Path("raw")
ARCHIVE_DIR = Path("archive")
OUTPUT_DIR = Path("outputs")
OUTPUT_CSV = OUTPUT_DIR / "images_metadata.csv"
BATCH_SIZE = 500

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}
VALID_SPLITS = {"train", "test", "unknown"}

def parse_local_parts(rel_dir_parts: Tuple[str, ...]): # Interpreta rutas del tipo: raw/<visibility>/<dataset>/<scenario?>/<split>/<...>/filename

    #**NORMAS DISPONIBLES EN LA CARPETA docs/**
    if not rel_dir_parts:
        return {"origin": None, "visibility": None, "dataset": None, "scenario": None, "split": None}

    origin = "raw"
    visibility = rel_dir_parts[0] if len(rel_dir_parts) > 0 else None
    dataset = rel_dir_parts[1] if len(rel_dir_parts) > 1 else None

    # Buscar el split por contenido (case-insensitive) a partir de índice 2.
    split = None
    split_idx = None
    for i in range(2, len(rel_dir_parts)):
        if rel_dir_parts[i].lower() in VALID_SPLITS:
            split = rel_dir_parts[i]
            split_idx = i
            break

    # Scenario es opcional: Entre dataset (idx=1) y el split encontrado.
    scenario = None
    if split_idx is not None and split_idx > 2:
        scenario = rel_dir_parts[2]

    return {
        "origin": origin,
        "visibility": visibility,
        "dataset": dataset,
        "scenario": scenario,
        "split": split,
    }

def derive_zip_name_from_dataset_local(dataset: Optional[str], scenario: Optional[str]) -> Optional[str]:
    """
    Busca en la carpeta local 'archive/' un zip asociado al dataset.
    - Si dataset == "youtube": <scenario>.zip
    - En cualquier otro caso: <dataset>_images.zip
    Comparación case-insensitive.
    Devuelve el nombre real del zip si existe, o None.
    """
    if not dataset:
        return None

    if dataset.lower() == "youtube":
        candidate_lower = f"{scenario}.zip".lower()
    else:
        candidate_lower = f"{dataset}_images.zip".lower()

    # Opción 1: Intento directo rápido.
    direct = ARCHIVE_DIR / candidate_lower
    if direct.is_file():
        return direct.name

    # Opción 2: Escaneo case-insensitive.
    if ARCHIVE_DIR.is_dir():
        for z in ARCHIVE_DIR.glob("*.zip"):
            if z.name.lower() == candidate_lower:
                return z.name

    return None

def image_size_from_path(p: Path): # Devuelve (width, height) si puede; si no, (None, None).
    try:
        with Image.open(p) as img:
            return img.width, img.height
    except Exception:
        return None, None

def compute_sha256_file(p: Path) -> Optional[str]: # Calcula el hash SHA-256 de un archivo.
    h = hashlib.sha256()
    try:
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def write_batch_csv(rows, header_written): # Escribe un lote de filas en el CSV.
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

        # Ruta relativa respecto a RAW_ROOT
        try:
            rel = p.relative_to(RAW_ROOT)
        except ValueError:
            continue

        rel_dir_parts = rel.parent.parts # Sin filename
        meta = parse_local_parts(rel_dir_parts)

        width, height = image_size_from_path(p)
        sha256 = compute_sha256_file(p)
        zip_name = derive_zip_name_from_dataset_local(meta["dataset"], meta["scenario"])

        # mtime como "ts_ingest" local (UTC)
        try:
            ts_ingest_dt = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        except Exception:
            ts_ingest_dt = datetime.now(timezone.utc)

        rows.append({
            "gcs_uri":   f"file://{p.resolve()}",
            "origin":    meta["origin"],
            "visibility": meta["visibility"],
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

    print(f"[SUCCESS] Completed. Total rows written: {total}")
    print(f"[CSV] {OUTPUT_CSV.resolve()}")
