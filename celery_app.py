from celery import Celery

celery_app = Celery(
    "worker",
    broker="redis://redis:6379/1",
    backend="redis://redis:6379/2",
    include=["tasks"]   # 🔥 ADD THIS LINE
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)
