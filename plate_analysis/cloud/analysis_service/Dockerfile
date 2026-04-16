FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender1 \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY cloud/analysis_service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY tools/ tools/
COPY weights/yolo_well_best.pt weights/yolo_well_best.pt
COPY cloud/analysis_service/main.py main.py

ENV WEIGHTS_PATH=/app/weights/yolo_well_best.pt

CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 8 --timeout 540 main:app"]
