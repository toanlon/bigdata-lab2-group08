FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN python -m pip install --no-cache-dir \
        duckdb==1.4.5 \
        boto3 \
        jsonschema \
    && python -m pip freeze > /opt/requirements.lock.txt \
    && mkdir -p /work \
    && chown 1000:1000 /work

WORKDIR /work

USER 1000:1000

CMD ["sleep", "infinity"]
