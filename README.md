# **Entorno local de pruebas para procesamiento automático de archivos `.zip`**

Este repositorio contiene una versión local del sistema de procesamiento automático de imágenes comprimidas en archivos `.zip`, pensada para **realizar pruebas, desarrollos y validaciones sin necesidad de desplegar en Google Cloud**.

> La rama `main` está destinada al entorno **de producción** y contiene el código adaptado para funcionar como una Google Cloud Function conectada al bucket `gs://svr_object_storage/`.

## **Funcionamiento del sistema**

Este flujo local simula lo que realiza la función en la nube:

1. **Lee archivos `.zip` desde la carpeta `archive/`**
2. **Descomprime su contenido**
3. **Valida y renombra imágenes según estructura esperada**
4. **Reorganiza las imágenes en la carpeta `silver/`**
5. **Registra un log del procesamiento en `logs/`**

## **Estructura del proyecto**

```plaintext
.
├── archive/            # Carpeta donde se colocan los archivos ZIP a procesar
├── raw/                # Carpeta de salida con las imágenes organizadas
├── logs/               # Logs generados automáticamente por cada ZIP procesado
├── main.py             # Código fuente (validador, renombrador, extractor)
├── requirements.txt    # Dependencias necesarias para ejecución local
├── .gcloudignore       # Reglas para despliegue en la nube (no aplica en local)
├── .gitignore
├── README.md
└── ZIP_ingestion_guidelines.md   # Normativa sobre la estructura de los `.zip`
```

## **Ejecución local**

1. Crea y activa un entorno virtual:

    ```bash
    python -m venv venv
    source venv/bin/activate   # o venv\Scripts\activate en Windows
    ```

2. Instala las dependencias:

    ```bash
    pip install -r requirements.txt
    ```

3. Ejecuta el procesamiento:

    ```bash
    python main.py
    ```

Esto procesará todos los archivos `.zip` encontrados en la carpeta `archive/`.

## **Reglas para los `.zip`**

Toda la normativa relativa a la estructura de los `.zip` y las convenciones del sistema se encuentra descrita en el archivo:

[`ZIP_ingestion_guidelines.md`](./ZIP_ingestion_guidelines.md)

## **Importante**

* Este entorno **no sube nada a Google Cloud**.
* Está pensado para validar datos, debuggear errores y verificar transformaciones antes de integrarlo en el pipeline real.
* La versión **oficial en producción** se encuentra en la rama `main`, conectada al bucket `gs://svr_object_storage/` y desplegada como Cloud Function.

## **Contacto**

Para dudas técnicas, mejoras o errores detectados, puedes abrir un *Issue* o contactar con el responsable del entorno de ingestión.
