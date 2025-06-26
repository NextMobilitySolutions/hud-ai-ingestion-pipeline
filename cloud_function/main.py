from google.cloud import storage
import os
from google.cloud import dataproc_v1

def already_processed(bucket_name, name): # Check if the log for the given file already exists.
    client = storage.Client()
    log_blob_name = f"logs/log_{os.path.basename(name)}.json"
    return client.bucket(bucket_name).blob(log_blob_name).exists()

def launch_dataproc_job(event, context): # Triggered by a change to a Cloud Storage bucket.
    import os
    bucket = event["bucket"]
    name = event["name"]

    if not name.startswith("archive/") or not name.endswith(".zip"): # Check if the file is in the 'archive/' folder and has a .zip extension.
        print("[SKIPPED] El archivo no es un ZIP en la carpeta 'archive/'.", name)
        return

    if already_processed(bucket, name): # Check if the file has already been processed.
        print(f"[SKIPPED] {name} ya fue procesado anteriormente (log existente).")
        return

    print(f"[START] Procesando {name} en el bucket {bucket}...")
    project_id = "braided-torch-459606-c6"
    region = "us-central1"
    cluster = "hud-processing-cluster"
    code_path = "gs://svr_object_storage/code/main.py"
    metadata_path = "gs://svr_object_storage/archive/config/youtube_metadata.json"

    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    job = {
        "placement": {"cluster_name": cluster},
        "pyspark_job": {
            "main_python_file_uri": code_path,
            "args": [
                "--zip_path", f"gs://{bucket}/{name}",
                "--bucket_name", bucket,
                "--silver_path", "raw/",
                "--logs_path", "logs/",
                "--youtube_metadata_path", metadata_path
            ]
        }
    }

    result = job_client.submit_job(project_id=project_id, region=region, job=job)
    print(f"[SUCCESS] Job submitted: {result.reference.job_id}")
