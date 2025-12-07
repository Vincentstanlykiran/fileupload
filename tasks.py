from celery_app import celery_app
from minio import Minio
import redis
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "files"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

@celery_app.task
def process_file_task(file_id, folder):
    """
    Example long-running file processing task.
    """
    try:
        obj = minio_client.get_object(MINIO_BUCKET, f"{folder}/{file_id}")
        size = obj.headers.get("Content-Length")

        # Store result
        redis_client.set(f"{file_id}:processed", size)

        return {"file_id": file_id, "processed_size": size}

    except Exception as e:
        return {"error": str(e)}
