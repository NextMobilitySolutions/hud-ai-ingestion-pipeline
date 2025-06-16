import os
import zipfile
import io
from PIL import Image
from datetime import datetime
import json
import pathlib

# Rutas principales.
ARCHIVE_PATH = "archive"
SILVER_PATH = "raw"
LOGS_PATH = "logs"
ALLOWED_EXTENSIONS = [".jpg", ".jpeg", ".png"]
YOUTUBE_METADATA_PATH = "youtube_metadata.json"
YOUTUBE_INFO = {}

# Cargar metadatos si existe.
if pathlib.Path(YOUTUBE_METADATA_PATH).is_file():
    with open(YOUTUBE_METADATA_PATH, "r", encoding="utf-8") as f:
        YOUTUBE_INFO = json.load(f)

# Asegurarse de que los directorios existen.
def ensure_dirs():
    os.makedirs(SILVER_PATH, exist_ok=True)
    os.makedirs(LOGS_PATH, exist_ok=True)
    os.makedirs(ARCHIVE_PATH, exist_ok=True)

# Verificar si el archivo es una imagen válida.
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

# Construir la ruta completa del archivo en el directorio raw.
def build_raw_path(dataset, scenario, split, file_name):
    origin = detect_origin(dataset) # Detectar el origen del dataset.
    if scenario: # Si hay un escenario, construir la ruta con el escenario.
        subpath = os.path.join(origin, dataset, scenario, split)
    else: # Si no hay escenario, construir la ruta sin él.
        subpath = os.path.join(origin, dataset, split)
    return os.path.join(SILVER_PATH, subpath, file_name)

# Procesar el archivo zip y extraer las imágenes.
def process_zip(zip_path):
    zip_name = os.path.basename(zip_path) # Obtener el nombre del archivo zip.
    processed, errors = 0, [] # Contadores para imágenes procesadas y errores.
    counter = {}

    with zipfile.ZipFile(zip_path, "r") as zipf: # Abrir el archivo zip.
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

                dest_path = build_raw_path(dataset, scenario, split, new_name) # Construir la ruta completa del archivo en el directorio raw.

                # Asegurarse de que la ruta de destino no exista, si existe, renombrar.
                base, ext = os.path.splitext(dest_path)
                duplicate_count = 1
                while os.path.exists(dest_path):
                    dest_path = f"{base}_{duplicate_count}{ext}"
                    duplicate_count += 1

                # Crear el directorio si no existe y guardar el archivo.
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                with open(dest_path, "wb") as out_file:
                    out_file.write(content)

                # Registrar el archivo procesado.
                processed += 1

            except Exception as e:
                errors.append(f"{file}: {str(e)}")

    # Crear el log de procesamiento.
    log = {
        "zip": zip_name,
        "fecha": datetime.utcnow().isoformat(),
        "procesadas": processed,
        "errores": errors,
    }

    # Añadir metadatos de YouTube si el .zip coincide con una entrada del diccionario.
    if zip_name in YOUTUBE_INFO:
        log["youtube_metadata"] = {
            "video_name": zip_name,
            "video_url": YOUTUBE_INFO[zip_name]["video_url"],
            "video_title": YOUTUBE_INFO[zip_name]["video_title"],
            "description": YOUTUBE_INFO[zip_name].get("description", "")
        }

    log_name = os.path.join(LOGS_PATH, f"log_{zip_name}.json")
    with open(log_name, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

    print(f"[SUCCESS] {zip_name} procesado: {processed} imágenes | {len(errors)} errores")

def main():
    ensure_dirs()
    for zip_file in os.listdir(ARCHIVE_PATH):
        if zip_file.endswith(".zip"):
            process_zip(os.path.join(ARCHIVE_PATH, zip_file))

if __name__ == "__main__":
    main()
