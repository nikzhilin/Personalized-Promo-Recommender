FROM python:3.12.10-slim-bookworm

RUN python -m pip install --no-cache-dir \
    fastapi==0.115.12 \
    prometheus-client==0.21.1 \
    pydantic==2.11.7 \
    'psycopg[binary]==3.2.9' \
    psycopg-pool==3.2.6 \
    redis==5.2.1 \
    uvicorn==0.34.2

WORKDIR /workspace
ENV PYTHONPATH=/workspace
USER 65532:65532

CMD ["python", "-m", "uvicorn", "services.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
