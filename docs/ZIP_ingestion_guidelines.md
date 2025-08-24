# **Normativa para la estructuración de archivos `.zip` en `gs://svr_object_storage/archive/`**

Todo archivo comprimido en formato `.zip` que se cargue en el bucket `archive/` deberá observar rigurosamente la estructura descrita a continuación. Este requisito es indispensable para que el sistema automático de ingestión y procesamiento pueda operar de forma eficiente y sin errores.

## **Estructura interna requerida**

```plaintext
[dataset]/[escenario]/[split]/[imagen]
```

**Definiciones:**

* **dataset**: denominación del conjunto de datos original (ej. `bdd100k`, `acdc`, `carla`, `youtube`)
* **escenario**: contexto o condición de captura (ej. `fog`, `curve`, `dirtroad_night`)
* **split**: partición correspondiente (`train`, `val`, `test`, o bien `unknown` si no aplica)

### **Ejemplos de estructuras válidas**

1. **Dataset con escenario y split definidos**

    ```plaintext
    acdc/fog/train/image_0001.png
    culane/curve/test/000123.jpg
    ```

2. **Dataset con escenario pero sin split**

    En aquellos casos en que el dataset incluya un escenario o contexto, pero no especifique una partición clara (`train`, `val`, `test`), deberá utilizarse expresamente la carpeta `unknown` como valor por defecto. En caso de omitirla, el sistema la asignará automáticamente durante el procesamiento.

    ```plaintext
    youtube/dirtroad_night_forest/unknown/frame_0001.jpg
    ```

3. **Dataset sin escenario**

    Algunos datasets, como `BDD100K` o `CARLA`, no segmentan por escenario:

    ```plaintext
    bdd100k/train/000abc.jpg
    carla/val/001.jpg
    ```

4. **Dataset sin escenario ni split (casos excepcionales)**

    Cuando no exista información contextual ni particional, se debe declarar explícitamente `unknown/unknown`:

    ```plaintext
    bdd100k/unknown/unknown/image.jpg
    ```

## **Condiciones obligatorias para los `.zip`**

* No se aceptarán imágenes ubicadas directamente en la raíz del `.zip`.
* Toda imagen deberá encontrarse, como mínimo, dentro de una subcarpeta identificada con el nombre del dataset.
* En ausencia de escenario o split, deberá utilizarse el término literal `unknown/`.
* Solo se admitirán archivos con extensión `.jpg`, `.jpeg` o `.png`.
* Se verificará que las imágenes sean legibles mediante bibliotecas estándar como PIL o OpenCV.
* El sistema descartará automáticamente:

  * Archivos no correspondientes a imágenes
  * Carpetas vacías
  * Rutas mal estructuradas

## **Estructura de salida tras el procesamiento automático**

Una vez procesado correctamente, cada imagen será reorganizada en el bucket de destino bajo la siguiente ruta:

```plaintext
raw/[origen]/[dataset]/[escenario]/[split]/[imagen]
```

Además, será renombrada siguiendo este formato normativo:

```plaintext
[dataset]_[escenario]_[split (si aplica)]_[######].[ext]
```

**Ejemplo ilustrativo:**

```plaintext
acdc_fog_test_00001.png
```

## **Registro de trazabilidad**

Cada lote procesado generará un archivo de registro (`log.json`) almacenado en la carpeta `logs/`, que incluirá:

* Nombre del archivo `.zip` procesado
* Número total de imágenes válidas
* Listado de errores (si los hubiera)
* Sello temporal de la operación

Este documento constituye una referencia técnica imprescindible para garantizar el correcto funcionamiento del flujo automático de procesamiento. Cualquier incumplimiento en la estructura aquí definida derivará en el rechazo total o parcial del contenido comprimido.
