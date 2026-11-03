FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METADWH_SQL_DIR=/app/sql

WORKDIR /app

COPY pyproject.toml ./
COPY metadwh ./metadwh
RUN pip install --no-cache-dir .

COPY sql ./sql

ENTRYPOINT ["metadwh"]
CMD ["--help"]
