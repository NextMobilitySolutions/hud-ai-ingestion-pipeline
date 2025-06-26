import os
import zipfile
import io
from PIL import Image
from datetime import datetime
import json
import pathlib
import argparse
from google.cloud import storage
from google.cloud.exceptions import NotFound

ALLOWED_EXTENSIONS = [".jpg", ".jpeg", ".png"]

def is_valid_image(file_bytes):
    try:
        with Image.open(io.BytesIO(file_bytes)) as img: # Open the image from bytes.
            img.verify()
        return True
    except Exception:
        return False

# Detectar el origen del dataset.
# Asume que los datasets simulados son de Carla y los públicos son de otro origen.
def detect_origin(dataset):
    return "simulated" if dataset.lower() == "carla" else "public"

# Extraer información del path del archivo.
# Asume que el formato del path es: dataset/scenario/split/file_name.ext
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

def gcs_path_join(*args):
    return "/".join(arg.strip("/") for arg in args)

# Construir la ruta completa del archivo en el directorio raw.
def build_raw_path(origin, dataset, scenario, split, file_name):
    if scenario: # Si hay un escenario, construir la ruta con el escenario.
        subpath = f"{origin}/{dataset}/{scenario}/{split}"
    else: # Si no hay escenario, construir la ruta sin él.
        subpath = f"{origin}/{dataset}/{split}"
    return f"{subpath}/{file_name}"

# Procesar el archivo zip y extraer las imágenes.
def process_zip(zip_bytes, zip_name, bucket_name, silver_path, logs_path, youtube_metadata):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    processed, errors = 0, []
    counter = {}
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zipf:
            for file in zipf.namelist(): # Iterar sobre los archivos en el zip.
                # Filtrar archivos que no son imágenes o directorios.
                if file.endswith("/") or not file.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
                    continue

                # Extraer información del path del archivo.
                try:
                    dataset, scenario, split = extract_path_info(file) # Extraer dataset, escenario y split.
                    key = f"{dataset}_{scenario or 'none'}_{split}" # Crear una clave única para el archivo.
                    counter[key] = counter.get(key, 0) + 1 # Contador para evitar duplicados.

                    ext = os.path.splitext(file)[-1].lower() # Obtener la extensión del archivo.
                    new_name = f"{key}_{counter[key]:05d}{ext}" # Renombrar el archivo con la clave y un contador.
                    content = zipf.read(file) # Leer el contenido del archivo.

                    if not is_valid_image(content):
                        raise Exception("Archivo no es imagen válida")

                    origin = detect_origin(dataset)
                    gcs_path = gcs_path_join(silver_path, build_raw_path(origin, dataset, scenario, split, new_name))

                    # Subir al bucket
                    blob = bucket.blob(gcs_path)
                    if blob.exists():
                        errors.append(f"{file}: skipped — file already exists in GCS as {gcs_path}")
                        continue  # Saltar este archivo, ya está en GCS

                    blob.upload_from_string(content)
                    processed += 1

                except Exception as e:
                    errors.append(f"{file}: {str(e)}")
    except zipfile.BadZipFile:
        raise Exception(f"[EXCEPTION] ZIP file is corrupted or invalid: {zip_name}")

    # Crear el log de procesamiento.
    log = {
        "zip": zip_name,
        "fecha": datetime.utcnow().isoformat(),
        "procesadas": processed,
        "errores": errors,
    }

    # Añadir metadatos de YouTube si el .zip coincide con una entrada del diccionario.
    if zip_name in youtube_metadata:
        log["youtube_metadata"] = youtube_metadata[zip_name]

    log_blob = bucket.blob(os.path.join(logs_path, f"log_{zip_name}.json"))
    log_blob.upload_from_string(json.dumps(log, indent=2, ensure_ascii=False), content_type='application/json')

    print(f"[SUCCESS] {zip_name} process: {processed} images | {len(errors)} errors")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip_path", required=True, help="Ruta al archivo .zip en GCS (gs://...)")
    parser.add_argument("--bucket_name", required=True, help="Nombre del bucket de GCS")
    parser.add_argument("--silver_path", required=True, help="Ruta base destino de imágenes en GCS")
    parser.add_argument("--logs_path", required=True, help="Ruta de logs en GCS")
    parser.add_argument("--youtube_metadata_path", default="youtube_metadata.json", help="Ruta al archivo JSON con metadatos (local o GCS)")
    args = parser.parse_args()

    storage_client = storage.Client()

    # Cargar metadatos
    youtube_metadata = {}
    if args.youtube_metadata_path.startswith("gs://"):
        # Leer metadata desde GCS
        path_parts = args.youtube_metadata_path.replace("gs://", "").split("/", 1)
        metadata_bucket = storage_client.bucket(path_parts[0])
        metadata_blob = metadata_bucket.blob(path_parts[1])
        if metadata_blob.exists():
            metadata_bytes = metadata_blob.download_as_bytes()
            youtube_metadata = json.loads(metadata_bytes.decode("utf-8"))
        else:
            print(f"[WARNING] Metadata file not found: {args.youtube_metadata_path}")
    elif os.path.isfile(args.youtube_metadata_path):
        # Leer metadata desde archivo local
        with open(args.youtube_metadata_path, "r", encoding="utf-8") as f:
            youtube_metadata = json.load(f)

    # Descargar ZIP desde GCS
    bucket = storage_client.bucket(args.bucket_name)
    zip_blob = bucket.blob(args.zip_path.replace(f"gs://{args.bucket_name}/", ""))
    try:
        zip_bytes = zip_blob.download_as_bytes()
    except NotFound:
        raise Exception(f"[EXCEPTION] ZIP file not found in GCS: {args.zip_path}")
    zip_name = os.path.basename(args.zip_path)

    # Procesar
    process_zip(zip_bytes, zip_name, args.bucket_name, args.silver_path, args.logs_path, youtube_metadata)

if __name__ == "__main__":
    main()
