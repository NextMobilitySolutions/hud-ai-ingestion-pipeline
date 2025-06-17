import os
import zipfile
import io
import tempfile
import json
from datetime import datetime
from PIL import Image
from google.cloud import storage

# Bucket y rutas
BUCKET_NAME = "svr_object_storage"
SILVER_PATH = "silver"
LOGS_PATH = "logs"
ALLOWED_EXTENSIONS = [".jpg", ".jpeg", ".png"]
YOUTUBE_METADATA_FILE = "archive/youtube_metadata.json"

# Cargar metadata de YouTube desde GCS
def load_youtube_metadata(bucket):
    try:
        blob = bucket.blob(YOUTUBE_METADATA_FILE)
        metadata_bytes = blob.download_as_bytes()
        return json.loads(metadata_bytes.decode("utf-8"))
    except Exception as e:
        print(f"[WARN] No se pudo cargar metadata de YouTube: {e}")
        return {}

# Verificar imagen válida
def is_valid_image(file_bytes):
    try:
        with Image.open(io.BytesIO(file_bytes)) as img:
            img.verify()
        return True
    except Exception:
        return False

# Detección de origen
def detect_origin(dataset):
    return "simulated" if dataset.lower() == "carla" else "public"

# Extraer ruta
def extract_path_info(path):
    parts = os.path.normpath(path).split(os.sep)
    dataset = parts[0]
    scenario, split = "", "unknown"

    if len(parts) >= 4 and parts[2] in ["train", "val", "test"]:
        scenario = parts[1]
        split = parts[2]
    elif len(parts) == 3 and parts[1] in ["train", "val", "test"]:
        scenario = ""
        split = parts[1]
    elif len(parts) == 3:
        scenario = parts[1]
        split = "unknown"
    elif len(parts) == 2:
        scenario = ""
        split = "unknown"

    return dataset, scenario, split

# Construcción de destino en silver/
def build_silver_path(dataset, scenario, split, file_name):
    origin = detect_origin(dataset)
    subpath = os.path.join(origin, dataset, scenario, split) if scenario else os.path.join(origin, dataset, split)
    return os.path.join(SILVER_PATH, subpath, file_name)

# Función principal
def process_new_zip(event, context):
    zip_name = event["name"]
    if not zip_name.lower().endswith(".zip") or not zip_name.startswith("archive/"):
        print(f"[SKIP] Ignorado: {zip_name}")
        return

    print(f"[START] Procesando: {zip_name}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(zip_name)

    youtube_info = load_youtube_metadata(bucket)
    processed, errors = 0, []
    counter = {}

    with tempfile.TemporaryDirectory() as temp_dir:
        zip_local_path = os.path.join(temp_dir, "archive.zip")
        blob.download_to_filename(zip_local_path)

        with zipfile.ZipFile(zip_local_path, "r") as zipf:
            for file in zipf.namelist():
                if file.endswith("/") or not file.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
                    continue

                try:
                    dataset, scenario, split = extract_path_info(file)
                    key = f"{dataset}_{scenario or 'none'}_{split}"
                    counter[key] = counter.get(key, 0) + 1

                    ext = os.path.splitext(file)[-1].lower()
                    new_name = f"{key}_{counter[key]:05d}{ext}"
                    content = zipf.read(file)

                    if not is_valid_image(content):
                        raise Exception("Archivo no válido")

                    dest_path = build_silver_path(dataset, scenario, split, new_name)
                    dest_blob = bucket.blob(dest_path)

                    # Verificación de duplicados
                    if dest_blob.exists():
                        i = 1
                        while bucket.blob(f"{os.path.splitext(dest_path)[0]}_{i}{ext}").exists():
                            i += 1
                        dest_path = f"{os.path.splitext(dest_path)[0]}_{i}{ext}"
                        dest_blob = bucket.blob(dest_path)

                    dest_blob.upload_from_string(content)

                    processed += 1

                except Exception as e:
                    errors.append(f"{file}: {str(e)}")

        # Log
        log = {
            "zip": zip_name,
            "fecha": datetime.utcnow().isoformat(),
            "procesadas": processed,
            "errores": errors
        }

        for key in youtube_info:
            if key in zip_name:
                log["youtube_metadata"] = {
                    "video_name": key,
                    "video_url": youtube_info[key]["video_url"],
                    "video_title": youtube_info[key]["video_title"],
                    "description": youtube_info[key].get("description", "")
                }
                break

        log_blob = bucket.blob(os.path.join(LOGS_PATH, f"log_{os.path.basename(zip_name)}.json"))
        log_blob.upload_from_string(json.dumps(log, indent=2), content_type="application/json")

    print(f"[SUCCESS] {zip_name} procesado: {processed} imágenes | {len(errors)} errores")
