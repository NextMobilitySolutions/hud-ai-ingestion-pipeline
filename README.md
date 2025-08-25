# **HUD AI Ingestion Pipeline**

Pipeline para **ingestión de archivos `.zip`** con imágenes y su reorganización en **Google Cloud Storage (GCS)**, pensado para uso **local/manual** y con una propuesta de ampliación a **Cloud Functions Gen 1 + Dataproc** (implementada pero **pendiente de despliegue**).

* **Ejecución local:** Procesamiento manual de ZIPs almacenados en GCS, subiendo directamente las imágenes validadas a la carpeta `raw/` y generando logs JSON en `logs/`.

* **Ampliación propuesta a Cloud Functions Gen 1:** Existe un entrypoint que, ante la llegada de un ZIP a `archive/`, **lanza un job** (p. ej., Dataproc / PySpark) para procesarlo. Esta función está **lista a nivel de código**, pero **no está desplegada a día** de hoy.

Nota: Toda la validación/renombrado ocurre en memoria; no se extraen archivos a disco local. Las imágenes se suben directamente a GCS con el nombre final.

## **Estructura del repositorio**

```plaintext
.
├── .github/workflows/
│   └── deploy-to-gcs.yml             # Workflow para sincronización automática con GCS
├── cloud_function/
│   ├── main.py                       # Cloud Function que lanza jobs en Dataproc
│   └── requirements.txt              # Dependencias necesarias para la función
├── config/
│   └── youtube_metadata.json         # Metadatos adicionales para datasets provenientes de vídeo
├── .gcloudignore                     # Archivos ignorados en despliegues GCP
├── .gitignore                        # Archivos ignorados por Git
├── LICENSE                           # Licencia Apache 2.0
├── README.md                         # Documentación técnica del proyecto
├── ZIP_ingestion_guidelines.md       # Guía para la estructuración correcta de archivos `.zip`
├── main.py                           # Script principal de procesamiento (Spark)
├── requirements.txt                  # Dependencias para ejecución local o en Dataproc
```

## **Estructura recomendada del bucket**

```plaintext
gs://<bucket>/
 ├── archive/          # Entrada: ZIPs originales
 ├── raw/              # Salida: imágenes validadas y renombradas
 ├── logs/             # Logs JSON por ZIP procesado
 └── config/
     └── youtube_metadata.json (opcional)
```

## **Ejecución local (CLI)**

**Objetivo:** Procesar manualmente un ZIP almacenado en GCS y subir imágenes renombradas a `raw/`, generando un log en `logs/`.

```powershell
python main.py --zip_path "gs://<bucket>/archive/lote_demo.zip" `
  --bucket_name "<bucket>" `
  --raw_path "raw" `
  --logs_path "logs" `
  --youtube_metadata_path "config/youtube_metadata.json"
```

**Salida esperada en consola:**

```python
[SUCCESS] lote_demo.zip process: 1200 images | 5 errors
```

**Ubicación de los resultados:**

* Imágenes: `gs://<bucket>/raw/<origin>/<dataset>/<scenario?>/<split>/...`
* Log del lote: `gs://<bucket>/logs/log_<zip_name>.json`

## **Convenciones del ZIP**

Los archivos comprimidos deben contener rutas estructuradas según la siguiente convención:

```plaintext
<dataset>/<scenario>/<split>/<archivo>
```

Valores aceptados:

* `split`: `train`, `val`, `test` (si no se detecta, se asigna `unknown`).
* `escenario`: es opcional (si falta, se asume vacío `None`).
* Imágenes aceptadas: `.jpg`, `.jpeg`, `.png` (ampliable)

Documentación completa disponible en [`ZIP_ingestion_guidelines.md`](./docs/ZIP_ingestion_guidelines.md).

Cada imagen se renombra como:

```plaintext
<dataset>_<scenario|none>_<split>_<contador>.ext
```

y se sube a:

```plaintext
raw/<origin>/<dataset>/<scenario?>/<split>/<nombre_normalizado.ext>
```

`origin` se determina a partir del dataset (p. ej., simulated si es carla, public en caso contrario).

## **Función de orquestación de jobs (Cloud Functions Gen 1 — propuesta)**

**Propósito:** Servir de capa de orquestación que, ante la llegada de un nuevo ZIP a `archive/`, remita un trabajo de procesamiento (PySpark) a Dataproc. La función no procesa imágenes por sí misma: únicamente valida el evento, verifica idempotencia y envía el job con los argumentos requeridos.

**Disparo y filtrado:** La función se define con trigger `google.storage.object.finalize` sobre el bucket objetivo. Debe ignorar cualquier objeto que (a) no resida bajo `archive/` y (b) no tenga extensión `.zip`.

**Idempotencia.** Antes de enviar el job, la función comprueba si existe `logs/log_<zip>.json`. Si existe, omite el envío (estado skipped) para evitar reprocesos del mismo ZIP.

**Envío del job.** La función construye un `pyspark_job` (Dataproc) cuya `main_python_file_uri` apunta al script de procesamiento en GCS (por ejemplo, `gs://…/code/main.py`). Se invoca el **JobController** regional de Dataproc y se registra el `job_id` retornado.

**Argumentos que inyecta al job.**

* `--zip_path gs://<bucket>/<name>`
* `--bucket_name <bucket>`
* `--silver_path raw`
* `--logs_path logs`
* `--youtube_metadata_path gs://<bucket>/archive/config/youtube_metadata.json`

**Requisitos.**

* El **cluster de Dataproc** debe existir en la **misma región** del JobController.
* La **cuenta de servicio** de la Cloud Function con permisos para **enviar jobs a Dataproc y leer GCS**. El job, a su vez, necesita permisos para leer `archive/` y escribir en `raw/` y `logs/`.
* El script de procesamiento (PySpark/Python) debe aceptar exactamente los argumentos anteriores.

**Buenas prácticas.**

* Validar nombre/ruta del objeto del evento antes de construir argumentos.
* Registrar claramente START, SUCCESS, SKIPPED y errores.
* Evitar auto‑disparos (por ejemplo, ubicando los ficheros de configuración bajo config/ en lugar de archive/).

### **Arquitectura funcional del sistema**

El siguiente diagrama ilustra el flujo automatizado de ingestión y procesamiento de archivos `.zip` en la infraestructura del proyecto HUD AI. Este pipeline está diseñado para operar de forma desatendida sobre Google Cloud Platform, desde la subida inicial del archivo hasta el registro final del procesamiento. La arquitectura contempla detección de eventos, ejecución distribuida y trazabilidad completa, garantizando escalabilidad, eficiencia y control de calidad en cada etapa.

![Arquitectura del sistema](./docs/architecture_pipeline.png)

### **Despliegue (propuesta) a Cloud Functions Gen 1 — pendiente**

Cuando se decida desplegar la ampliación:

1. Revise y parametrice las variables en el código de la función (project/region/cluster/rutas) y súbelas a variables de entorno o configúralas en un archivo de settings.
2. Despliega con gcloud (ejemplo genérico):

```powershell
gcloud functions deploy hud-zip-trigger \
--entry-point launch_dataproc_job \
--runtime python311 \
--trigger-event google.storage.object.finalize \
--trigger-resource <bucket> \
--region <region> \
--timeout 540s \
--memory 512MB
```

3. Cargue un archivo ZIP en `gs://<bucket>/archive/` para probar. La función debe ignorar archivos que no sean .zip o que no estén en archive/.

**Recomendaciones:** controle los duplicados revisando si existe `logs/log_<zip>.json` antes de lanzar el job; registra `START/SUCCESS/SKIPPED` para trazabilidad.

### **Script de procesamiento en Spark**

El archivo `main.py` ya está adaptado para ejecución en Dataproc y acepta argumentos mediante `argparse`. Funcionalidad principal:

* Acceso directo al ZIP desde GCS
* Descompresión en memoria
* Validación y renombrado de imágenes
* Organización en rutas tipo `raw/public/dataset/escenario/split/`
* Generación de log estructurado en `logs/`

### **Automatización con GitHub Actions**

El repositorio incluye un flujo de trabajo automático (`.github/workflows/deploy-to-gcs.yml`) que sincroniza los archivos `main.py` y `requirements.txt` a la carpeta `code/` del bucket de GCS al realizar `push` sobre `main` por si se amplia a este plan.

## **Licencia**

Este proyecto está licenciado bajo los términos de la licencia Apache 2.0. Consultar el archivo `LICENSE` para más información.
