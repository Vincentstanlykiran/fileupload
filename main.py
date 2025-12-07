from fastapi import FastAPI, UploadFile, HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi_jwt import JwtAccessBearer, JwtAuthorizationCredentials
from minio import Minio
import redis
import uuid
import os
from pydantic import BaseModel
from io import BytesIO

app = FastAPI(title="FastAPI + MinIO + Redis Microservice")

# -------------------------------
# JWT Setup
# -------------------------------

SECRET_KEY = "supersecretjwtkey"
access_security = JwtAccessBearer(secret_key=SECRET_KEY)

class LoginModel(BaseModel):
    username: str
    password: str

AUTH_USERNAME = "admin"
AUTH_PASSWORD = "password123"

@app.post("/login")
def login(data: LoginModel):
    if data.username != AUTH_USERNAME or data.password != AUTH_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = access_security.create_access_token(
        subject={"username": data.username}  # Fastapi_jwt requires dict
    )

    return {"access_token": token}


# Authentication dependency
def authenticate(credentials: JwtAuthorizationCredentials = Depends(access_security)):
    return True


# -------------------------------
# MinIO Configuration
# -------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "files"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure bucket exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)


# -------------------------------
# Redis Configuration
# -------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)


# -------------------------------
# Upload File
# -------------------------------
@app.post("/upload")
async def upload_file(folder: str, file: UploadFile, auth=Depends(authenticate)):
    try:
        file_id = str(uuid.uuid4())
        file_data = await file.read()

        object_name = f"{folder}/{file_id}"

        # convert bytes to stream
        stream = BytesIO(file_data)

        minio_client.put_object(
            MINIO_BUCKET,
            object_name,
            data=stream,
            length=len(file_data),
            content_type=file.content_type
        )

        redis_client.set(file_id, f"{folder}|{file.filename}")

        return {"file_id": file_id, "folder": folder, "filename": file.filename}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------
# Get File Metadata
# -------------------------------
@app.get("/file/{file_id}")
async def get_file_metadata(file_id: str, auth=Depends(authenticate)):
    try:
        data = redis_client.get(file_id)
        if not data:
            raise HTTPException(status_code=404, detail="File ID not found")

        folder, filename = data.decode().split("|")
        object_name = f"{folder}/{file_id}"
        response = minio_client.get_object(MINIO_BUCKET, object_name)

        return {
            "file_id": file_id,
            "folder": folder,
            "filename": filename,
            "size": response.headers.get("Content-Length"),
            "content_type": response.headers.get("Content-Type")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------
# Download File
# -------------------------------
@app.get("/download/{file_id}")
async def download_file(file_id: str, auth=Depends(authenticate)):
    try:
        data = redis_client.get(file_id)
        if not data:
            raise HTTPException(status_code=404, detail="File ID not found")

        folder, filename = data.decode().split("|")
        object_name = f"{folder}/{file_id}"

        file_data = minio_client.get_object(MINIO_BUCKET, object_name)

        return StreamingResponse(
            file_data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
