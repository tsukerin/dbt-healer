FROM python:3.12-slim-bookworm
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SERVICE_PORT=8888

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install dbt-core dbt-postgres

COPY . .

RUN apt-get update && apt-get install -y git

EXPOSE 8888
CMD ["sh", "-c", "uvicorn service.failure_ingest:app --host 0.0.0.0 --port ${SERVICE_PORT}"]