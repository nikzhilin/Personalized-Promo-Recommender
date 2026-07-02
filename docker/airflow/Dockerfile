FROM spark:3.5.8-scala2.12-java17-python3-ubuntu@sha256:a71719262ca64125b520ec37e80d8689d1b393d91d4e7733f9740fb4ca5e26ed AS spark-client

FROM apache/airflow:2.10.5-python3.11

USER root
COPY --from=spark-client /opt/spark /opt/spark
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN python -m pip install --no-cache-dir \
    hdfs==2.7.3 \
    catboost==1.2.8 \
    numpy==1.26.4 \
    pandas==2.2.3 \
    psycopg2-binary==2.9.10 \
    psycopg[binary]==3.2.9 \
    pyarrow==18.1.0 \
    pydantic==2.11.7 \
    PyYAML==6.0.2 \
    redis==5.2.1 \
    scikit-learn==1.5.2
