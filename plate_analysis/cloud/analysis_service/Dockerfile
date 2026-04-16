# cloud/analysis_service/Dockerfile
#
# Build context: repo root  (/home/rami/plate_analysis/)
#
# Before building, copy the YOLO weights into the repo:
#   cp /home/rami/yolo_well_dataset/runs/detect/well/weights/best.pt \
#      /home/rami/plate_analysis/weights/yolo_well_best.pt
#
# Build:
#   docker build \
#     -f cloud/analysis_service/Dockerfile \
#     -t gcr.io/YOUR_PROJECT/analysis-service:latest \
#     .
#
# Push:
#   docker push gcr.io/YOUR_PROJECT/analysis-service:latest
#
# Deploy (first time):
#   gcloud run deploy analysis-service \
#     --image gcr.io/YOUR_PROJECT/analysis-service:latest \
#     --region us-central1 \
#     --no-allow-unauthenticated \
#     --set-env-vars GCS_BUCKET=...,GCP_PROJECT=...,QUEUE_LOCATION=...,QUEUE_ID=...,CLOUD_RUN_URL=...,CLOUD_RUN_SA_EMAIL=...

FROM python:3.12-slim

# System dependencies for OpenCV headless + ultralytics
RUN apt-get update && apt-get install -y --no-install-recommends \
        libglib2.0-0 \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-caching friendly)
COPY cloud/analysis_service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline tools (spot_assay.py, yolo_color_pipeline.py, hough_grid_annotate.py)
COPY tools/ tools/

# Bake model weights into the image so no runtime download is needed.
# weights/yolo_well_best.pt must exist in the build context.
COPY weights/yolo_well_best.pt weights/yolo_well_best.pt

# Copy the service entrypoint
COPY cloud/analysis_service/main.py main.py

ENV WEIGHTS_PATH=/app/weights/yolo_well_best.pt

# Cloud Run injects $PORT at runtime (default 8080).
# Shell-form CMD expands $PORT; exec replaces the shell so gunicorn
# receives SIGTERM directly.
CMD exec gunicorn \
      --bind "0.0.0.0:${PORT:-8080}" \
      --workers 1 \
      --timeout 540 \
      --preload \
      main:app
