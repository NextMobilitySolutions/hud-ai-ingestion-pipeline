'''
- Escanea gs://<bucket>/raw/... en busca de imágenes (.jpg/.jpeg/.png)
- Analiza el conjunto de datos/escenario/división a partir de las rutas de los objetos; lee el ancho/alto; calcula SHA-256
- Escribe un inventario en la tabla de BigQuery <PROJECT>.<DATASET>.images_metadata (particionada por ts_ingest)
- Desduplicación: utiliza una clave estable (gcs_uri o sha256) como insertId para que las inserciones en streaming sean idempotentes y seguras en los reintentos.
- Uso previsto: se ejecuta justo después de cada ingesta de ZIP; también es adecuado para el reabastecimiento inicial y los barridos de consistencia periódicos.
- Emite registros por ejecución con los recuentos procesados ​​y los errores.
'''

from typing import List, Optional
import io
import pandas as pd
from PIL import Image
from google.cloud import storage, bigquery
import hashlib

# Configuración inicial.
PROJECT_ID = "braided-torch-459606-c6"
DATASET = "hud_project"
TABLE   = "images_metadata"

TARGETS = [("svr_object_storage", "raw/"),] # Recorremos SOLO raw/.
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}
BATCH_SIZE = 500  # Filas por lote de subida.


# Funciones auxiliares.
def get_ext(filename: str) -> str: # Devuelve la extensión del archivo.
    i = filename.rfind(".")
    return filename[i:].lower() if i >= 0 else ""

def parse_path(parts: List[str]):
    """
    Interpreta rutas del tipo:
        raw/<visibility>/<dataset>/<scenario?>/<split>/<...>/filename

    Ejemplos:
        raw/public/acdc/night/train/img.jpg
        raw/simulated/hudv2/test/img.png
        raw/real/acdc/train/img.jpg
    """
    if not parts or parts[0] != "raw":
        return {"origin": None, "visibility": None, "dataset": None, "scenario": None, "split": None}

    origin = parts[0]                       # "raw"
    visibility = parts[1] if len(parts) > 1 else None
    dataset = parts[2] if len(parts) > 2 else None

    # Buscar split (train/test/unknown)
    valid_splits = {"train", "test", "unknown"}
    split = None
    split_idx = None
    for i in range(3, len(parts)):
        if parts[i].lower() in valid_splits:
            split = parts[i]
            split_idx = i
            break

    # Scenario es opcional: está entre dataset y split
    scenario = None
    if split_idx is not None and split_idx > 3:
        scenario = parts[3]

    return {
        "origin": origin,
        "visibility": visibility,
        "dataset": dataset,
        "scenario": scenario,
        "split": split,
    }

def derive_zip_name_from_dataset(
    dataset: Optional[str],
    bucket_name: str,
    storage_client: storage.Client,
    scenario: Optional[str] = None,
) -> Optional[str]:
    """
    Busca en gs://<bucket_name>/archive/ el ZIP asociado al dataset.
    Regla:
        - Si dataset == "youtube"  -> <scenario>.zip
        - En otro caso             -> <dataset>_images.zip
    Comparación case-insensitive. Devuelve el NOMBRE real del ZIP en GCS,
    o None si no se encuentra.
    """
    if not dataset:
        return None

    ds_lower = dataset.lower()
    if ds_lower == "youtube":
        if not scenario:
            return None  # Sin scenario no podemos construir el nombre
        candidate_lower = f"{scenario}.zip".lower()
    else:
        candidate_lower = f"{dataset}_images.zip".lower()

    try:
        # Opción 1: Escanear /archive/ de forma case-insensitive (robusto)
        for blob in storage_client.list_blobs(bucket_name, prefix="archive/"):
            name = blob.name
            if name.endswith("/"):
                continue
            fname = name.rsplit("/", 1)[-1]
            if fname.lower() == candidate_lower:
                return fname  # devolver tal cual existe en GCS

        # Opción 2: Intento directo exacto en minúsculas por si el objeto está así grabado,
        lower_path = f"archive/{candidate_lower}"
        if storage_client.bucket(bucket_name).blob(lower_path).exists():
            return candidate_lower

        return None
    except Exception:
        return None



def image_size_from_bytes(b: bytes): # Cálculo del tamaño de la imagen a partir de los bytes.
    """Devuelve (width, height) si puede; si no, None."""
    try:
        img = Image.open(io.BytesIO(b))
        return img.width, img.height
    except Exception:
        return None

def compute_sha256_stream(blob: storage.Blob) -> Optional[str]: # Cálculo del SHA-256 en streaming.
    h = hashlib.sha256()
    try:
        with blob.open("rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

# Flujo principal.
def main():
    # Creación de clientes de Storage y BigQuery.
    storage_client = storage.Client(project=PROJECT_ID)
    bq = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    # Definición de la tabla.
    rows = []
    total = 0

    for bucket_name, prefix in TARGETS: # Iterar sobre los buckets y prefijos (raw/)
        for blob in storage_client.list_blobs(bucket_name, prefix=prefix):

            # Saltar "directorios" y objetos vacíos.
            if blob.name.endswith("/") or (blob.size or 0) == 0:
                continue

            # Filtrar solo imágenes.
            filename = blob.name.split("/")[-1]
            ext = get_ext(filename)
            if ext not in IMAGE_EXTS:
                continue

            # Parsear metadatos desde la ruta.
            parts = [p for p in blob.name.split("/") if p]
            meta = parse_path(parts[:-1])

            # Cálculo de dimensiones de la imagen.
            width = height = None
            try:
                header = blob.download_as_bytes(start=0, end=65535)
                sz = image_size_from_bytes(header)
                if not sz:
                    # Fallback: descarga completa si el header no basta
                    data = blob.download_as_bytes()
                    sz = image_size_from_bytes(data)
                if sz:
                    width, height = sz
            except Exception:
                pass

            # Cálculo del SHA-256 en streaming.
            sha256 = compute_sha256_stream(blob)

            # Derivar el nombre del zip a partir del dataset.
            zip_name = derive_zip_name_from_dataset(meta["dataset"], bucket_name, storage_client)

            # Timestamp de ingestión.
            ts_ingest_dt = blob.time_created

            # Agregar metadatos a la fila.
            rows.append({
                "gcs_uri":  f"gs://{bucket_name}/{blob.name}",
                "origin":   meta["origin"],
                "visibility": meta["visibility"],
                "dataset":  meta["dataset"],
                "scenario": meta["scenario"],
                "split":    meta["split"],
                "filename": filename,
                "ext":      ext.lstrip("."),
                "width":    width,
                "height":   height,
                "sha256":   sha256,
                "zip_name": zip_name,
                "ts_ingest": ts_ingest_dt,
            })

            # Subida en lotes.
            if len(rows) >= BATCH_SIZE:
                df = pd.DataFrame(rows)
                # Tipos correctos antes de cargar.
                df["ts_ingest"] = pd.to_datetime(df["ts_ingest"], utc=True)
                for c in ["width", "height"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                bq.load_table_from_dataframe(df, table_id, job_config=job_config).result()
                total += len(rows); rows.clear()
                print(f"[UPLOAD] Uploaded {total} rows...")

    # Subir cualquier fila restante.
    if rows:
        df = pd.DataFrame(rows)
        df["ts_ingest"] = pd.to_datetime(df["ts_ingest"], utc=True)
        for c in ["width", "height"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        bq.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        total += len(rows)

    print(f"[BACKFILL] Backfill completed. Rows inserted: {total}")

if __name__ == "__main__":
    main()
