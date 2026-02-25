web: uvicorn src.api.main:app --host 0.0.0.0 --port $PORT
worker: celery -A src.worker.celery_app worker --loglevel=info --concurrency=1
