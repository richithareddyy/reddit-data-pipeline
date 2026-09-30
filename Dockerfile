FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

COPY pyproject.toml ./
COPY sql ./sql
COPY pipeline ./pipeline
COPY tests ./tests

RUN useradd --create-home --uid 1000 etl && mkdir -p /data && chown etl:etl /data
USER etl

ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["--help"]
